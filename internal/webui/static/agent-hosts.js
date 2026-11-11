(function (root) {
  "use strict";
  const storageKey = "kpl.host-reboot-request.v1";
  const escape = value => String(value ?? "").replace(/[&<>"']/g, char => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[char]);
  const activeStates = new Set(["queued", "running"]);
  const terminalStates = new Set(["completed", "failed", "interrupted"]);
  const hostStates = new Set(["queued", "checking", "requested", "rebooted", "unknown", "rejected"]);
  const labels = {queued:"Queued", checking:"Checking host", requested:"Reboot requested", rebooted:"Reboot confirmed", unknown:"Outcome unknown", rejected:"Not requested"};
  const disabledReason = "Host reboot is disabled on this Agent.";
  const enableHint = "Enable host reboot with kpl config set KPL_AGENT_HOST_REBOOT_ENABLED=true, then apply it through a planned Agent redeployment.";
  const validAgentIdentity = agent => typeof agent?.host?.bootId === "string" && agent.host.bootId
    && typeof agent.startedAt === "string" && Number.isFinite(Date.parse(agent.startedAt)) && !agent.startedAt.startsWith("0001-");

  function groupHosts(agents) {
    const groups = new Map();
    for (const agent of Array.isArray(agents) ? agents : []) {
      const nodeId = agent?.host?.nodeId;
      if (typeof nodeId !== "string" || !nodeId) continue;
      if (!groups.has(nodeId)) groups.set(nodeId, {nodeId, agents:[]});
      groups.get(nodeId).agents.push(agent);
    }
    for (const group of groups.values()) {
      const agents = group.agents.sort((a,b) => String(a.id).localeCompare(String(b.id)));
      const live = agents.filter(agent => agent.state === "online");
      const boots = new Set(live.map(agent => agent.host.bootId).filter(Boolean));
      const eligible = agents.find(agent => agent.state === "online" && agent.host.rebootEnabled === true
        && agent.host.rebootSupported === true && validAgentIdentity(agent) && !agent.host.rebootPending);
      // Disabled Agents have not probed host support yet. Let an operator
      // select and review that known host without granting reboot permission.
      // An explicit unsupported-host reason still keeps selection blocked.
      const configurable = agents.find(agent => agent.state === "online" && agent.host.rebootEnabled === false
        && validAgentIdentity(agent) && !agent.host.rebootPending
        && (agent.host.rebootSupported === true || !agent.host.rebootReason || agent.host.rebootReason === disabledReason));
      const candidate = eligible || configurable;
      const conflictingBoots = boots.size > 1;
      const rebootPending = agents.some(agent => agent.host.rebootPending && (!candidate || agent.host.bootId === candidate.host.bootId));
      group.agent = candidate || live[0] || agents[0];
      group.hostname = group.agent.host.hostname || group.agent.hostname || group.nodeId;
      group.reason = conflictingBoots ? "Host boot identity changed. Refresh Agents before selecting this host."
        : rebootPending ? "A host reboot is already pending."
        : eligible ? "" : group.agent.state !== "online" ? "The Agent is offline."
          : !validAgentIdentity(group.agent) ? "Host or Agent identity is unavailable. Refresh Agents or update the Agent image."
          : configurable ? disabledReason + " " + enableHint
          : group.agent.host.rebootReason || "Host reboot is unavailable or its identity is unknown.";
      group.available = !group.reason;
      group.selectable = !!candidate && !conflictingBoots && !rebootPending;
    }
    return groups;
  }

  function validStoredRequest(value) {
    const payload = value?.payload;
    return value?.version === 1 && typeof payload?.id === "string" && /^[a-zA-Z0-9][a-zA-Z0-9_-]{7,127}$/.test(payload.id)
      && payload.confirm === true && Array.isArray(payload.targets) && payload.targets.length > 0 && payload.targets.length <= 128
      && new Set(payload.targets.map(target => target?.nodeId)).size === payload.targets.length
      && payload.targets.every(target => [target?.agentId,target?.nodeId,target?.bootId,target?.agentStartedAt]
        .every(field => typeof field === "string" && field.length > 0 && field.length <= 256 && !/[\r\n\0]/.test(field)));
  }

  function createAgentHosts(environment = root) {
    const document = environment.document;
    const timer = environment.setTimeout || setTimeout, clearTimer = environment.clearTimeout || clearTimeout;
    let options, initialized = false, groups = new Map(), draft = null, receipt = null, inspection = null;
    let submitting = false, polling = false, pollTimer = null, focus = null, unidentifiedAgents = 0;
    const selected = new Map();
    const element = id => document?.querySelector?.("#" + id);
    const text = (id,value) => { const target=element(id); if(target)target.textContent=value; };
    const hidden = (id,value) => { const target=element(id); if(target)target.hidden=value; };
    const pending = () => receipt?.uncertain || activeStates.has(receipt?.job?.state);
    const targetFor = group => ({agentId:group.agent.id,nodeId:group.nodeId,bootId:group.agent.host.bootId,agentStartedAt:group.agent.startedAt});
    const agentLabel = id => id ? options?.agentLabel?.(id) || "" : "";

    function store() {
      try { environment.sessionStorage?.setItem(storageKey, JSON.stringify(receipt)); } catch {}
    }
    function notice(message,error=false) {
      text("agentHostStatus",message);hidden("agentHostStatus",!message);
      element("agentHostStatus")?.setAttribute?.("role",error?"alert":"status");
    }
    function unavailable(group) {
      if (!group) return "The host is no longer in the Agent list.";
      if (group.reason) return group.reason;
      const blocked = receipt?.job?.hosts?.find(host => host.nodeId === group.nodeId && host.bootId === group.agent.host.bootId
        && ["requested","unknown"].includes(host.state));
      return blocked ? "A previous reboot request is not yet confirmed. Check the host before requesting another reboot." : "";
    }
    function selectionUnavailable(group) {
      if (!group || !group.selectable) return unavailable(group);
      const blocked = receipt?.job?.hosts?.find(host => host.nodeId === group.nodeId && host.bootId === group.agent.host.bootId
        && ["requested","unknown"].includes(host.state));
      return blocked ? "A previous reboot request is not yet confirmed. Check the host before requesting another reboot." : "";
    }
    function selectionError(requireReady = true) {
      for (const [nodeId,target] of selected) {
        const group = groups.get(nodeId), reason = requireReady ? unavailable(group) : selectionUnavailable(group);
        if (reason) return reason;
        const current = targetFor(group);
        if (current.agentId !== target.agentId || current.bootId !== target.bootId || current.agentStartedAt !== target.agentStartedAt) {
          return "A selected Agent or host restarted. Clear and select the hosts again.";
        }
      }
      return "";
    }
    function render() {
      const available = [...groups.values()].filter(group => !selectionUnavailable(group));
      const count = available.filter(group => selected.has(group.nodeId)).length;
      const master = element("selectAgentHosts");
      if (master) {
        master.disabled = submitting || !available.length;
        master.checked = available.length > 0 && count === available.length;
        master.indeterminate = count > 0 && count < available.length;
        master.setAttribute?.("aria-checked",master.indeterminate?"mixed":String(master.checked));
      }
      const invalid = [...selected.keys()].filter(nodeId => unavailable(groups.get(nodeId))).length;
      text("agentHostSelection",selected.size + " host" + (selected.size===1?"":"s") + " selected" + (invalid?" · "+invalid+" not ready":""));
      const reboot = element("rebootAgentHosts");
      if (reboot) {
        reboot.disabled = !selected.size || submitting || !!pending() || !!selectionError(false);
        reboot.title = pending() ? "Resolve the current reboot request before starting another." : selectionError(false) || selectionError() || "Reboot the selected Linux hosts after earlier queue work finishes.";
      }
      const selectionReason = selectionError();
      const unavailableHosts = [...groups.values()].filter(group => unavailable(group));
      const explanation = unavailableHosts.find(group => group.selectable) || unavailableHosts[0];
      const availability = selectionReason || (!selected.size && unavailableHosts.length
        ? unavailableHosts.length + " host" + (unavailableHosts.length === 1 ? " is" : "s are") + " not ready for reboot. " + unavailable(explanation)
        : !selected.size && unidentifiedAgents ? "Some Agents do not report a host identity. Update the Agent image and refresh Agents before selecting their hosts." : "");
      text("agentHostAvailability", availability);hidden("agentHostAvailability", !availability);
      const clear = element("clearAgentHosts");if(clear)clear.disabled=!selected.size||submitting;
      hidden("viewHostReboot",!receipt);
      for (const checkbox of document?.querySelectorAll?.("[data-host-select]") || []) {
        const group = groups.get(checkbox.dataset.hostSelect), reason = checkbox.dataset.hostSelect ? selectionUnavailable(group) : "This Agent does not report a host identity for reboot.";
        const label = agentLabel(checkbox.dataset.hostAgent || group?.agent.id);
        const identity = [label, group?.hostname].filter(Boolean).join(" · ");
        checkbox.checked = selected.has(checkbox.dataset.hostSelect);
        checkbox.disabled = submitting || !!reason && !checkbox.checked;
        checkbox.title = (checkbox.checked ? "Deselect Linux host " : "Select Linux host ") + identity + (reason || unavailable(group) ? ". " + (reason || unavailable(group)) : "");
      }
    }
    function checkbox(agent) {
      const nodeId = agent?.host?.nodeId || "", group = groups.get(nodeId), reason = nodeId ? selectionUnavailable(group) : "This Agent does not report a host identity for reboot.";
      const name = group?.hostname || agent.hostname || agent.name || agent.id;
      const label = agentLabel(agent.id), identity = [label, name].filter(Boolean).join(" · ");
      const title = (selected.has(nodeId) ? "Deselect Linux host " : "Select Linux host ") + identity + (reason || unavailable(group) ? ". " + (reason || unavailable(group)) : "");
      return '<input class="agent-host-checkbox" type="checkbox" data-host-select="'+escape(nodeId)+'" data-host-agent="'+escape(agent.id)+'" aria-label="'+escape("Select Linux host "+(label||name)+(group?.agents.length>1?" (shared by "+group.agents.length+" Agents)":""))+'" title="'+escape(title)+'"'+(selected.has(nodeId)?' checked':'')+(submitting||reason&&!selected.has(nodeId)?' disabled':'')+'>';
    }
    function update(agents) {
      groups=groupHosts(agents);
      unidentifiedAgents=(Array.isArray(agents)?agents:[]).filter(agent=>typeof agent?.host?.nodeId!=="string"||!agent.host.nodeId).length;
      render();if(element("agentHostDialog")?.open)renderDialog();
    }
    function beforeRender() {
      const active=document?.activeElement,table=active?.closest?.(".table-wrap");
      focus=active?.dataset?.hostSelect?{nodeId:active.dataset.hostSelect,agentId:active.dataset.hostAgent,table,left:table?.scrollLeft,top:table?.scrollTop}:null;
    }
    function afterRender() {
      render();
      if (!focus) return;
      const target=[...(document?.querySelectorAll?.("[data-host-select]")||[])].find(item=>item.dataset.hostSelect===focus.nodeId&&item.dataset.hostAgent===focus.agentId);
      (target&&!target.disabled?target:element("clearAgentHosts"))?.focus?.({preventScroll:true});
      if(focus.table){focus.table.scrollLeft=focus.left;focus.table.scrollTop=focus.top;}
      focus=null;
    }
    function select(nodeId,checked) {
      if(submitting)return;
      if(!checked){selected.delete(nodeId);render();return;}
      const group=groups.get(nodeId);
      if(!group||selectionUnavailable(group))return;
      if(selected.size>=128&&!selected.has(nodeId)){notice("Select at most 128 hosts.",true);render();return;}
      selected.set(nodeId,targetFor(group));
      render();
    }
    function show() {
      const dialog=element("agentHostDialog");if(dialog&&!dialog.open)dialog.showModal?.();
      if(draft&&!submitting&&!receipt?.uncertain)element("closeAgentHostDialog")?.focus?.();
    }
    function hostMarkup(host) {
      const group=groups.get(host.nodeId),label=agentLabel(host.agentId||group?.agent.id),hostname=host.hostname||group?.hostname||"";
      return '<li><strong>'+escape(label||hostname||host.nodeId)+'</strong>'+(label&&hostname?'<span class="agent-id">'+escape(hostname)+'</span>':'')+'<code>'+escape(host.nodeId)+'</code>'
        +(host.state?'<span class="host-reboot-state" data-state="'+escape(host.state)+'">'+escape(labels[host.state]||host.state)+'</span>':'')
        +(host.error?'<span class="host-reboot-error">'+escape(host.error)+'</span>':'')+'</li>';
    }
    function renderDialog() {
      const job=inspection||receipt?.job, list=element("agentHostTargets"), uncertain=!inspection&&receipt?.uncertain;
      const hosts=job?.hosts||draft?.targets||receipt?.payload?.targets||[];
      if(list){const markup=hosts.map(hostMarkup).join("");if(list.innerHTML!==markup)list.innerHTML=markup;}
      text("agentHostDialogSummary", job ? "Request "+job.id+" · "+job.state : hosts.length+" Linux host"+(hosts.length===1?"":"s")+" selected.");
      text("agentHostDialogStatus",(uncertain?"The response was not confirmed. Refresh status or retry this exact request; a new reboot request will not be generated."
        : job?.state==="queued"?"Queued behind earlier experiments and analysis."
        : job?.state==="running"?"Reboot work is running. A request acknowledgment does not confirm a completed reboot."
        : job?"New reboot requests automatically restore previous participation settings after a verified host return. Review any host errors; unknown outcomes stay excluded and require checking the host."
        : selectionError() || "The Controller will check the selected host identities again before requesting reboot.")+(job?.error?" "+String(job.error):""));
      const confirm=element("confirmAgentHostReboot");
      if(confirm){confirm.hidden=!!inspection||!!job&&!uncertain;confirm.disabled=submitting||!uncertain&&!job&&!!selectionError();confirm.textContent=submitting?"Submitting…":uncertain?"Retry same request":"Queue reboot";}
      const close=element("closeAgentHostDialog");if(close){close.disabled=submitting;close.textContent=job||receipt?.uncertain?"Close":"Cancel";}
      const refresh=element("refreshHostReboot");if(refresh){refresh.hidden=!receipt&&!inspection;refresh.disabled=submitting||polling;}
    }
    function open(id) {
      if(typeof id==="string")return openJob(id);
      if(submitting||pending()||!selected.size)return;
      const error=selectionError(false);if(error){notice(error,true);return;}
      draft={targets:[...selected.values()].sort((a,b)=>a.nodeId.localeCompare(b.nodeId)).map(target=>({...target,hostname:groups.get(target.nodeId)?.hostname}))};
      receipt=null;inspection=null;text("agentHostDialogError","");renderDialog();show();
    }
    function schedulePoll() {
      if(pollTimer!=null)clearTimer(pollTimer);
      if(activeStates.has(inspection?.state||receipt?.job?.state))pollTimer=timer(()=>{pollTimer=null;void refresh();},3000);
    }
    function validJob(job,id) {
      return job?.id===id&&(activeStates.has(job.state)||terminalStates.has(job.state))
        &&Array.isArray(job.hosts)&&job.hosts.every(host=>hostStates.has(host.state));
    }
    function acceptJob(job) {
      if(!receipt||!validJob(job,receipt.payload.id))throw new Error("Unexpected host reboot status response.");
      receipt.job=job;receipt.uncertain=false;store();render();renderDialog();schedulePoll();
      notice(activeStates.has(job.state)?"Host reboot work is "+job.state+". You can close the dialog.":"Host reboot work finished: "+job.state+". Review each host's outcome.",job.state!=="completed"&&!activeStates.has(job.state));
    }
    async function request(path,configuration={}) {
      const controller=new AbortController(),timeout=timer(()=>controller.abort(),30000);
      try{return await options.api(path,{...configuration,signal:controller.signal});}finally{clearTimer(timeout);}
    }
    async function submit() {
      if(submitting||inspection||(!draft&&!receipt?.uncertain))return;
      if(!receipt?.uncertain){
        const error=selectionError();if(error){text("agentHostDialogError",error);return;}
        const id="host-reboot-"+(environment.crypto?.randomUUID?.()||Date.now().toString(36)+"-"+Math.random().toString(36).slice(2));
        receipt={version:1,payload:{id,targets:draft.targets.map(({hostname,...target})=>target),confirm:true},uncertain:true};
      }
      const id=receipt.payload.id;
      // Preserve the exact target boot IDs and operation identity before sending.
      // Reloads and uncertain replies may only retry this same payload manually.
      store();submitting=true;text("agentHostDialogError","");render();renderDialog();
      try{
        const job=await request("/api/v1/agent-hosts/reboot",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(receipt.payload)});
        if(receipt?.payload.id!==id)return;
        acceptJob(job);selected.clear();draft=null;render();
      }catch(error){
        if(receipt?.payload.id!==id)return;
        if(error.accepted===false&&[400,404,409,422,503].includes(error.status)){
          receipt=null;store();text("agentHostDialogError",error.message||"The reboot request was not admitted.");
          notice("The reboot request was not admitted. Review the selected hosts and try again.",true);return;
        }
        // A response can be lost after durable admission. Never replace its ID
        // or assume that a reboot did not happen, including after an HTTP error.
        receipt.uncertain=true;store();
        text("agentHostDialogError",error.name==="AbortError"?"The request timed out. Its outcome is unknown; refresh status before retrying.":error.message||"Could not confirm the reboot request.");
        notice("Host reboot request outcome is not confirmed. Open Reboot status to resolve it.",true);
      }finally{submitting=false;render();renderDialog();}
    }
    async function refresh() {
      if((!receipt&&!inspection)||submitting||polling)return;
      polling=true;renderDialog();const id=inspection?.id||receipt.payload.id;
      try{
        const job=await request("/api/v1/agent-hosts/reboot/"+encodeURIComponent(id));
        if(inspection?.id===id){if(!validJob(job,id))throw new Error("Unexpected host reboot status response.");inspection=job;renderDialog();schedulePoll();}
        else if(receipt?.payload.id===id)acceptJob(job);
        text("agentHostDialogError","");
      }
      catch(error){if(inspection?.id===id||receipt?.payload.id===id){text("agentHostDialogError",error.status===404?"No saved request was found. Refresh the queue before retrying any reboot request.":error.message||"Could not refresh reboot status.");schedulePoll();}}
      finally{polling=false;renderDialog();}
    }
    async function openJob(id) {
      if(!id)return;
      if(receipt?.payload.id===id){draft=null;inspection=null;renderDialog();show();void refresh();return;}
      try{
        const job=await request("/api/v1/agent-hosts/reboot/"+encodeURIComponent(id));
        if(!validJob(job,id))throw new Error("Unexpected host reboot status response.");
        // Inspecting another tab's work must never replace this tab's durable
        // original operation identity after an uncertain submission response.
        inspection=job;draft=null;text("agentHostDialogError","");renderDialog();show();schedulePoll();
      }catch(error){notice(error.message||"Could not open host reboot status.",true);}
    }
    function init(configuration) {
      options=configuration;if(initialized)return;initialized=true;
      try{const raw=environment.sessionStorage?.getItem(storageKey);const saved=raw&&raw.length<=524288?JSON.parse(raw):null;if(validStoredRequest(saved)){receipt=saved;receipt.uncertain=true;}}catch{}
      document?.addEventListener?.("change",event=>{
        const control=event.target;if(control?.dataset?.hostSelect!==undefined)select(control.dataset.hostSelect,control.checked);
        else if(control?.id==="selectAgentHosts"){const checked=control.checked;for(const group of groups.values())if(!selectionUnavailable(group))select(group.nodeId,checked);}
      });
      document?.addEventListener?.("click",event=>{
        const button=event.target.closest?.("#rebootAgentHosts, #clearAgentHosts, #viewHostReboot, #confirmAgentHostReboot, #closeAgentHostDialog, #refreshHostReboot");
        if(!button||button.disabled)return;event.preventDefault();
        if(button.id==="rebootAgentHosts")open();
        else if(button.id==="clearAgentHosts"){selected.clear();render();}
        else if(button.id==="viewHostReboot"){draft=null;inspection=null;renderDialog();show();void refresh();}
        else if(button.id==="confirmAgentHostReboot")void submit();
        else if(button.id==="refreshHostReboot")void refresh();
        else{
          element("agentHostDialog")?.close?.();
          inspection=null;schedulePoll();
          const target=element(receipt?"viewHostReboot":"rebootAgentHosts");
          (target&&!target.disabled?target:element("clearAgentHosts"))?.focus?.({preventScroll:true});
        }
      });
      element("agentHostDialog")?.addEventListener?.("cancel",event=>{if(submitting)event.preventDefault();});
      element("agentHostDialog")?.addEventListener?.("close",()=>{inspection=null;schedulePoll();});
      render();if(receipt)void refresh();
    }
    return {init,update,checkbox,beforeRender,afterRender,open,openJob};
  }
  if(typeof module!=="undefined"&&module.exports)module.exports={createAgentHosts,groupHosts,validStoredRequest};
  else root.KPLAgentHosts=createAgentHosts();
})(typeof globalThis!=="undefined"?globalThis:this);
