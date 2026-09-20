"use strict";
const form = document.querySelector("#loginForm");
const submit = document.querySelector("#loginSubmit");
const error = document.querySelector("#loginError");
let submitting = false;

async function checkSession() {
  try {
    const response = await fetch("/api/v1/auth/session", { credentials: "same-origin", cache: "no-store" });
    if (response.ok) location.replace("/");
  } catch { /* The form remains available if the connection is interrupted. */ }
}

form.addEventListener("submit", async event => {
  event.preventDefault();
  if (submitting || !form.reportValidity()) return;
  submitting = true;
  submit.disabled = true;
  submit.textContent = "Logging in…";
  error.textContent = "";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const response = await fetch("/api/v1/auth/login", {
      method: "POST", credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user: document.querySelector("#loginUser").value, password: document.querySelector("#loginPassword").value }),
      signal: controller.signal,
    });
    if (!response.ok) {
      const result = await response.json().catch(() => ({}));
      throw new Error(result.error || "Could not log in. Please try again.");
    }
    document.querySelector("#loginPassword").value = "";
    location.replace("/");
  } catch (failure) {
    error.textContent = failure.name === "AbortError" ? "Login timed out. Please try again." : failure.message;
  } finally {
    clearTimeout(timeout);
    submitting = false;
    submit.disabled = false;
    submit.textContent = "Log in";
  }
});
window.addEventListener("pageshow", event => { if (event.persisted) void checkSession(); });
try { localStorage.removeItem("kpl-api-token"); } catch { /* Legacy credential cleanup. */ }
void checkSession();
