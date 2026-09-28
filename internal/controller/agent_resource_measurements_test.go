package controller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const measurementsPath = "/api/v1/agents/resources/measurements"

func measurementResponse(t *testing.T, response *httptest.ResponseRecorder) resourceMeasurementList {
	t.Helper()
	if response.Code != 200 && response.Code != 201 {
		t.Fatalf("measurement response: %d %s", response.Code, response.Body)
	}
	var data resourceMeasurementList
	if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestResourceMeasurementLifecycleRestartAndOldStop(t *testing.T) {
	config := ServerConfig{DataDir: t.TempDir()}
	s := New(config, nil)
	first := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	if first.ID == "" || first.StartedAt.IsZero() || !first.EndedAt.IsZero() {
		t.Fatalf("invalid start %+v", first)
	}
	const clients = 12
	ids := make(chan string, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := resultRequest(s, "POST", measurementsPath)
			var data resourceMeasurementList
			if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &data) != nil || len(data.Measurements) != 1 {
				ids <- "invalid"
				return
			}
			ids <- data.Measurements[0].ID
		}()
	}
	wg.Wait()
	close(ids)
	for id := range ids {
		if id != first.ID {
			t.Fatal("concurrent starts created another interval")
		}
	}
	export := measurementsPath + "/" + first.ID + "/export?format=csv"
	if r := resultRequest(s, "GET", export); r.Code != 409 {
		t.Fatalf("active export: %d", r.Code)
	}
	s = New(config, nil)
	restored := measurementResponse(t, resultRequest(s, "GET", measurementsPath)).Measurements[0]
	if restored != first {
		t.Fatalf("restart changed interval %+v", restored)
	}
	stop := measurementsPath + "/" + first.ID + "/stop"
	ended := measurementResponse(t, resultRequest(s, "POST", stop)).Measurements[0]
	if ended.EndedAt.IsZero() || ended.EndReason != "stopped" || !ended.StartedAt.Equal(first.StartedAt) {
		t.Fatalf("invalid stop %+v", ended)
	}
	if got := measurementResponse(t, resultRequest(s, "POST", stop)).Measurements[0]; got != ended {
		t.Fatal("retry changed end boundary")
	}
	newer := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	if newer.ID == first.ID {
		t.Fatal("new measurement reused ID")
	}
	data := measurementResponse(t, resultRequest(s, "POST", stop))
	if data.Measurements[0] != newer || data.Measurements[1] != ended {
		t.Fatal("old stop altered current measurement")
	}
	s = New(config, nil)
	data = measurementResponse(t, resultRequest(s, "GET", measurementsPath))
	if len(data.Measurements) != 2 || data.Measurements[1] != ended {
		t.Fatal("completed interval not persisted")
	}
	if r := resultRequest(s, "POST", measurementsPath+"/missing/stop"); r.Code != 404 {
		t.Fatal("unknown stop succeeded")
	}
}

func TestResourceMeasurementFixedFractionalWindowAndShortEmptyExport(t *testing.T) {
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	from, to := base.Add(250*time.Millisecond), base.Add(625*time.Millisecond)
	var queryTimes []string
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if !strings.HasSuffix(r.Form.Get("query"), "[1s]") {
			t.Errorf("short interval query: %s", r.Form.Get("query"))
		}
		queryTimes = append(queryTimes, r.Form.Get("time"))
		values := [][2]any{}
		for i, offset := range []time.Duration{100, 250, 500, 625, 750} {
			values = append(values, [2]any{float64(base.Unix()) + float64(offset)/1000, strconv.Itoa(i + 1)})
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{"agent_id": "worker", "__name__": "kpl_agent_cpu_usage_percent"}, "values": values}}}})
	}))
	defer prom.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	m := resourceMeasurement{ID: strings.Repeat("a", 32), StartedAt: from, EndedAt: to, EndReason: "stopped"}
	if err := s.saveResourceMeasurementsLocked([]resourceMeasurement{m}); err != nil {
		t.Fatal(err)
	}
	s.resourceMeasurementsLoaded = true
	path := measurementsPath + "/" + m.ID + "/export?format=csv"
	for range 2 {
		r := resultRequest(s, "GET", path+"&kind=summary")
		rows, err := csv.NewReader(r.Body).ReadAll()
		if err != nil || r.Code != 200 || len(rows) != 2 {
			t.Fatalf("summary %d %v %v", r.Code, rows, err)
		}
		if rows[1][4] != from.Format(time.RFC3339Nano) || rows[1][5] != to.Format(time.RFC3339Nano) || rows[1][8] != "2" || rows[1][9] != "3.500000" || rows[1][11] != "4.000000" {
			t.Fatalf("wrong interval samples: %v", rows)
		}
		if !strings.Contains(r.Header().Get("Content-Disposition"), m.ID) {
			t.Fatal("filename lacks measurement ID")
		}
	}
	if queryTimes[0] != queryTimes[1] {
		t.Fatal("repeat export moved the interval")
	}
	r := resultRequest(s, "GET", path)
	rows, err := csv.NewReader(r.Body).ReadAll()
	if err != nil || len(rows) != 3 {
		t.Fatalf("sample CSV: %v %v", rows, err)
	}
	s.resourceMeasurements[0].StartedAt = base.Add(800 * time.Millisecond)
	s.resourceMeasurements[0].EndedAt = base.Add(900 * time.Millisecond)
	if r := resultRequest(s, "GET", path); r.Code != 404 {
		t.Fatalf("empty short interval: %d %s", r.Code, r.Body)
	}
	if r := resultRequest(s, "GET", path+"&kind=invalid"); r.Code != 400 {
		t.Fatal("invalid format accepted")
	}
}

func TestResourceMeasurementExpiryRetentionAndStorageFailure(t *testing.T) {
	dir := t.TempDir()
	s := New(ServerConfig{DataDir: dir}, nil)
	first := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	s.resourceMeasurements[0].StartedAt = time.Now().UTC().Add(-25 * time.Hour)
	expired := measurementResponse(t, resultRequest(s, "GET", measurementsPath)).Measurements[0]
	if expired.EndReason != "time_limit" || expired.EndedAt.Sub(expired.StartedAt) != 24*time.Hour {
		t.Fatalf("expiry %+v", expired)
	}
	data := measurementResponse(t, resultRequest(s, "POST", measurementsPath))
	if data.Measurements[0].ID == first.ID || data.Measurements[1] != expired {
		t.Fatal("expired measurement blocks next interval")
	}
	// Refuse a failed atomic replacement and preserve the retryable active state.
	path := filepath.Join(dir, resourceMeasurementFile)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	current := data.Measurements[0]
	stop := measurementsPath + "/" + current.ID + "/stop"
	if r := resultRequest(s, "POST", stop); r.Code != 503 {
		t.Fatalf("storage failure: %d", r.Code)
	}
	if got := measurementResponse(t, resultRequest(s, "GET", measurementsPath)).Measurements[0]; got != current {
		t.Fatal("failed save changed live state")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	measurementResponse(t, resultRequest(s, "POST", stop))
	for i := 0; i < resourceMeasurementLimit+2; i++ {
		m := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
		measurementResponse(t, resultRequest(s, "POST", measurementsPath+"/"+m.ID+"/stop"))
	}
	data = measurementResponse(t, resultRequest(New(ServerConfig{DataDir: dir}, nil), "GET", measurementsPath))
	if len(data.Measurements) != resourceMeasurementLimit {
		t.Fatalf("index not bounded: %d", len(data.Measurements))
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	broken := New(ServerConfig{DataDir: dir}, nil)
	if r := resultRequest(broken, "POST", measurementsPath); r.Code != 503 {
		t.Fatal("corrupt index overwritten")
	}
	bytes, _ := os.ReadFile(path)
	if string(bytes) != "corrupt" {
		t.Fatal("discarded unreadable history")
	}
}

func TestResourceMeasurementAuthenticationAndUnavailableHistory(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "interval-test"}, nil)
	for _, req := range [][2]string{{"GET", measurementsPath}, {"POST", measurementsPath}, {"POST", measurementsPath + "/abc/stop"}, {"DELETE", measurementsPath + "/abc"}, {"GET", measurementsPath + "/abc/export"}} {
		if r := resultRequest(s, req[0], req[1]); r.Code != 401 {
			t.Fatalf("unauthorized %v: %d", req, r.Code)
		}
	}
	// Mutations retain browser session and same-origin CSRF protection.
	cookie := loginCookie(t, s)
	for _, path := range []string{measurementsPath, measurementsPath + "/abc/stop"} {
		req := httptest.NewRequest("POST", path, nil)
		req.AddCookie(cookie)
		response := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(response, req)
		if response.Code != 403 {
			t.Fatalf("CSRF mutation accepted: %d", response.Code)
		}
	}
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer prom.Close()
	s = New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	m := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	stopped := measurementResponse(t, resultRequest(s, "POST", measurementsPath+"/"+m.ID+"/stop")).Measurements[0]
	if r := resultRequest(s, "GET", measurementsPath+"/"+m.ID+"/export?format=csv"); r.Code != 502 {
		t.Fatal("unavailable history produced CSV")
	}
	if got := measurementResponse(t, resultRequest(s, "GET", measurementsPath)).Measurements[0]; got != stopped {
		t.Fatal("failed download lost interval")
	}
}

func TestResourceMeasurementDeletePersistsAndProtectsActiveIntervals(t *testing.T) {
	dir := t.TempDir()
	s := New(ServerConfig{DataDir: dir}, nil)
	m := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	path := measurementsPath + "/" + m.ID
	if r := resultRequest(s, "DELETE", path); r.Code != http.StatusConflict {
		t.Fatalf("active delete: %d", r.Code)
	}
	measurementResponse(t, resultRequest(s, "POST", path+"/stop"))
	newer := measurementResponse(t, resultRequest(s, "POST", measurementsPath)).Measurements[0]
	deleted := measurementResponse(t, resultRequest(s, "DELETE", path))
	if len(deleted.Measurements) != 1 || deleted.Measurements[0].ID != newer.ID || !deleted.Measurements[0].EndedAt.IsZero() {
		t.Fatalf("deleted wrong interval: %+v", deleted)
	}
	restored := New(ServerConfig{DataDir: dir}, nil)
	if got := measurementResponse(t, resultRequest(restored, "GET", measurementsPath)); len(got.Measurements) != 1 || got.Measurements[0].ID != newer.ID {
		t.Fatal("deletion did not survive restart")
	}
	for _, method := range []string{"DELETE", "GET"} {
		endpoint := path
		if method == "GET" {
			endpoint += "/export?format=csv"
		}
		if r := resultRequest(restored, method, endpoint); r.Code != http.StatusNotFound {
			t.Fatalf("deleted record is still accessible: %s %d", method, r.Code)
		}
	}
	measurementResponse(t, resultRequest(restored, "POST", measurementsPath+"/"+newer.ID+"/stop"))
	if err := os.Remove(filepath.Join(dir, resourceMeasurementFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, resourceMeasurementFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if r := resultRequest(restored, "DELETE", measurementsPath+"/"+newer.ID); r.Code != http.StatusServiceUnavailable {
		t.Fatalf("storage failure: %d", r.Code)
	}
	if got := measurementResponse(t, resultRequest(restored, "GET", measurementsPath)); len(got.Measurements) != 1 {
		t.Fatal("failed deletion changed live state")
	}
}
