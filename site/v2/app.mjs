import {renderPathways} from "./pathways.mjs";
import {renderDestinationTreemaps} from "./treemaps.mjs";
import {VIEWS, makeQuery, sections, label, format, visibleKey,degreeLabel,normalizeEducation} from "./contracts.mjs";
import {chartModels} from "./chart-models.mjs";
import {renderChart,fitCharts} from "./charts.mjs";
import {actionFor,chartAction} from "./report-models.mjs";
import {comparisonModels,comparisonRows,comparisonScope,comparisonSections} from "./comparison-models.mjs";
import {createReports} from "./reports.mjs";
export function startWorkspace({request,apiOrigin="",pathwaysOrigin=apiOrigin}={}) {
const responseCache=new Map();
const $ = id => document.getElementById(id);
const state = {view:"overview", mode:"snapshot", schools:[], degree:"Bachelors", major:"", years:[], horizon:"1yr",peers:[],majors:[],extraMajors:[],compareDimension:"school",comparisonTab:"overview",cipLevel:"cip4"};
let token="", options=null, busy=false,connectionEpoch=0;const activeRequests=new Set();
function cancelPending(){connectionEpoch++;for(const controller of activeRequests)controller.abort();activeRequests.clear();busy=false;reports.close();$("content").removeAttribute("aria-busy");controls(!options);$("login-form").querySelector("[type=submit]").disabled=false;}
const reports=createReports({api,getOptions:()=>options});
const welcome=$("content").innerHTML;
const icons=["▦","↗","◇","⇢","⌁","▤","◎","⇢","◉","≍","◌"];
Object.entries(VIEWS).forEach(([key,[title]],index)=>{
  const button=document.createElement("button");button.dataset.view=key;
  const icon=document.createElement("span");icon.className="nav-icon";icon.textContent=icons[index];button.append(icon,document.createTextNode(title));
  button.addEventListener("click",()=>activateView(key));$("nav").append(button);
});
function updateHeading(){
  document.querySelector(".heading .mode").hidden=state.view==="pathways";$("horizon").closest("label").hidden=state.view==="pathways";
  $("title").textContent=VIEWS[state.view][0];$("subtitle").textContent=VIEWS[state.view][1];
  document.querySelectorAll("[data-view]").forEach(b=>{if(b.dataset.view===state.view)b.setAttribute("aria-current","page");else b.removeAttribute("aria-current");});
}
function setStatus(message,error=false){$("status").textContent=message;$("status").classList.toggle("error",error);}
function controls(disabled){document.querySelectorAll(".filters select,.comparison-controls select,#apply,#open-comparison,[data-mode],[data-view],[data-compare],[data-analysis]").forEach(el=>el.disabled=disabled);}
function comparisonControls(){
  const compare=state.view==="comparison",major=state.compareDimension==="major";
  $("comparison-controls").hidden=!compare;$("peer-fields").hidden=major;$("major-fields").hidden=!major;$("open-comparison").hidden=compare;
  $("major-label").textContent=compare&&major?"Major 1":state.cipLevel==="cip2"?"Program area":"Program";
  $("compare-help").textContent=major?"Choose two or three majors within this institution. Degree, cohort and horizon stay the same.":"Compare your institution with one or two peers. The same program, cohort and horizon apply to each.";
  document.querySelectorAll("[data-compare]").forEach(b=>b.setAttribute("aria-pressed",String(b.dataset.compare===state.compareDimension)));
  document.querySelectorAll("[data-analysis]").forEach(b=>b.setAttribute("aria-pressed",String(b.dataset.analysis===state.comparisonTab)));
}
async function activateView(key){if(busy)return;const wasComparison=state.view==="comparison";if(options)collect();invalidate();state.view=key;comparisonControls();updateHeading();if(options){if(wasComparison!==(key==="comparison"))await refreshOptions();await load();}}
function invalidate(){if(!options||busy)return;reports.close();$("content").replaceChildren(element("section","Your cohort changed. Explore the cohort to load its results.","empty"));setStatus("Filters changed. Results will update when you explore this cohort.");}
async function api(path,body){
  const authAtStart=token,epochAtStart=connectionEpoch;
  const cacheKey=["/api/dashboard","/api/pathways"].includes(path)?path+JSON.stringify(body):null;
  const cached=cacheKey&&responseCache.get(cacheKey);if(cached&&Date.now()-cached.time<120000)return cached.data;
  if(request){const data=await request(path,body);if(authAtStart!==token||epochAtStart!==connectionEpoch)throw new Error("The data connection changed.");if(cacheKey)responseCache.set(cacheKey,{time:Date.now(),data});return data;}
  const origin=path==="/api/pathways"?pathwaysOrigin:apiOrigin;
  if(origin===null)throw new Error("Role pathways need a protected hosted data service. They remain available in the local tracker; no pathway data has been published to GitHub Pages.");
  const controller=new AbortController();activeRequests.add(controller);try{const response=await fetch(origin+path,{signal:controller.signal,method:body?"POST":"GET",headers:body?{"Content-Type":"application/json","X-Outcomes-Password":token}:{},body:body?JSON.stringify(body):undefined});
  const data=await response.json();if(!response.ok){const detail=data.detail;throw new Error(typeof detail==="string"?detail:`Query failed (${response.status}).`);}if(authAtStart!==token||epochAtStart!==connectionEpoch)throw new Error("The data connection changed. Open the report again.");if(cacheKey){responseCache.set(cacheKey,{time:Date.now(),data});if(responseCache.size>12)responseCache.delete(responseCache.keys().next().value);}return data;}finally{activeRequests.delete(controller);}
}
function populate(id, rows, value, title, initial){
  const select=$(id);select.replaceChildren();for(const row of rows){const option=document.createElement("option");option.value=String(value(row));option.textContent=String(title(row));select.append(option);}if(initial!==undefined&&[...select.options].some(o=>o.value===String(initial)))select.value=String(initial);
}
function fillOptions(first=false){
  const schools=options.schools||[];
  populate("school",schools,r=>r.unitid,r=>r.name,state.schools[0]);
  if(first){$("school").value=schools.some(r=>String(r.unitid)==="123165")?"123165":String(schools[0]?.unitid||"");}
  populate("peer",[{unitid:"",name:"Choose a peer"},...schools.filter(r=>String(r.unitid)!==$("school").value)],r=>r.unitid,r=>r.name,state.peers[0]||"");
  populate("peer2",[{unitid:"",name:"Add another peer"},...schools.filter(r=>String(r.unitid)!==$("school").value)],r=>r.unitid,r=>r.name,state.peers[1]||"");
  populate("degree",options.degrees||[],r=>r.degree,r=>degreeLabel(r.label||r.degree),state.degree);
  populate("major",[{code:"",title:state.view==="comparison"&&state.compareDimension==="major"?"Choose first major":"All programs"},...(options.major_options||[])],r=>r.code,r=>r.title,state.major);
  populate("years",[{value:"",title:"All available cohorts"},...(options.grad_years||[]).slice().sort((a,b)=>b-a).map(y=>({value:y,title:String(y)}))],r=>r.value,r=>r.title,state.years[0]||"");
  state.cipLevel=options.meta?.major_option_cip_level||"cip4";
  populate("compare-major",[{code:"",title:"Choose another major"},...(options.major_options||[])],r=>r.code,r=>r.title,state.extraMajors[0]||"");
  populate("compare-major2",[{code:"",title:"Add another major"},...(options.major_options||[])],r=>r.code,r=>r.title,state.extraMajors[1]||"");
  comparisonControls();
}
function collect(){
  state.peers=[$("peer").value,$("peer2").value];
  state.schools=[$("school").value,...(state.view==="comparison"&&state.compareDimension==="school"?state.peers.filter(Boolean):[])];
  state.degree=$("degree").value;state.major=$("major").value;state.extraMajors=[$("compare-major").value,$("compare-major2").value];state.majors=[state.major,...state.extraMajors].filter(Boolean);
  state.years=$("years").value?[Number($("years").value)]:[];state.horizon=$("horizon").value;
  const majorNames=state.view==="comparison"&&state.compareDimension==="major"?state.majors.map(code=>options.major_options?.find(r=>String(r.code)===code)?.title||code):[state.major?$("major").selectedOptions[0].textContent:"All programs"];
  $("cohort-label").textContent=[...state.schools.map(s=>(options.schools||[]).find(r=>String(r.unitid)===s)?.name||s),...majorNames].join(" · ");
}
async function refreshOptions(){
  if(busy||!token)return;const epoch=connectionEpoch;invalidate();collect();const before={major:state.major,majors:[...state.majors],extraMajors:[...state.extraMajors],years:[...state.years],cipLevel:state.cipLevel};busy=true;controls(true);setStatus("Updating available cohort options…");
  try{const q=makeQuery({...state,cipLevel:"cip4"},{optionsOnly:true});options=await api("/api/options",q);const level=options.meta?.major_option_cip_level||"cip4";
    if(level!==before.cipLevel){state.major="";state.majors=[];state.extraMajors=[];}else{const available=new Set((options.major_options||[]).map(r=>String(r.code)));state.major=available.has(before.major)?before.major:"";state.extraMajors=before.extraMajors.map(code=>available.has(code)?code:"");}
    state.years=before.years.filter(y=>(options.grad_years||[]).includes(y));fillOptions();collect();setStatus("Cohort options updated. Explore to load results.");
  }catch(error){if(epoch===connectionEpoch)setStatus(error.message,true);}finally{if(epoch===connectionEpoch){busy=false;controls(false);}}
}
for(const id of ["school","peer","peer2","degree"])$(id).addEventListener("change",refreshOptions);
for(const id of ["major","compare-major","compare-major2","years","horizon"])$(id).addEventListener("change",invalidate);
$("open-comparison").addEventListener("click",()=>activateView("comparison"));
for(const button of document.querySelectorAll("[data-compare]"))button.addEventListener("click",async()=>{if(busy)return;if(options)collect();state.compareDimension=button.dataset.compare;comparisonControls();if(options)await refreshOptions();});
for(const button of document.querySelectorAll("[data-analysis]"))button.addEventListener("click",async()=>{if(busy)return;state.comparisonTab=button.dataset.analysis;comparisonControls();if(options)await load();});
function element(tag,text,className){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(className)e.className=className;return e;}
function metricGrid(metrics,onMetric){const grid=element("div",undefined,"metrics");for(const [key,value] of Object.entries(metrics)){if(typeof value!=="number"||!visibleKey(key))continue;const item=element("div",undefined,"metric");item.append(element("small",label(key)),element("strong",format(value,key)));if(onMetric){item.classList.add("clickable-metric");item.setAttribute("role","button");item.tabIndex=0;item.setAttribute("aria-label",`Open ${label(key)} report`);item.addEventListener("click",()=>onMetric(key));item.addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();onMetric(key);}});}grid.append(item);}return grid;}
function table(rows,section,data,q,hideKeys=[]){
  const keys=[...new Set(rows.flatMap(row=>Object.keys(row)))].filter(k=>!hideKeys.includes(k)&&visibleKey(k)&&!rows.some(r=>r[k]&&typeof r[k]==="object"));
  const wrap=element("div",undefined,"table-wrap"), t=element("table"),thead=element("thead"),head=element("tr");
  for(const key of keys){const th=element("th",key==="title"&&section==="major_comparison"?"Major":key==="title"&&section==="school_comparison"?"Institution":label(key));th.scope="col";head.append(th);}thead.append(head);t.append(thead);
  const body=element("tbody");for(const row of rows){const tr=element("tr");let linked=false;for(const key of keys){const td=element("td");if(section&&!linked&&typeof row[key]==="string"&&!["code","unitid"].includes(key)){const action=actionFor(section,row,data),button=element("button",format(row[key],key),"report-link");button.setAttribute("aria-label",`View report: ${action.title||row[key]}`);button.addEventListener("click",()=>reports.open(action,q,data));td.append(button);linked=true;}else td.textContent=format(row[key],key);tr.append(td);}body.append(tr);}t.append(body);wrap.append(t);return wrap;
}
function metricReport(key,data,q){const tab=key.includes("salary")?"earnings":key==="later_degree_pct"?"postgrad":key==="unique_employers"?"employers":"coverage";reports.open({kind:"report",tab,title:label(key)+" report"},q,data);}
function render(data,q){
  data=normalizeEducation(data);
  const content=$("content");content.replaceChildren();const compare=state.view==="comparison",list=compare?comparisonSections(data,q):sections(data);
  const evidence=element("div",undefined,"evidence");const meta=data.meta||{};
  evidence.textContent=`Source: ${meta.data_version||"version not returned"} · ${q.horizon==="early_2025"||meta.partial_horizon?"Partial outcome horizon":"Horizon: "+$("horizon").selectedOptions[0].textContent} · ${q.view_mode==="snapshot"?"Snapshot":"Over time"}.`;
  content.append(evidence);
  if(state.view==="pathways"){evidence.textContent="Linked role checkpoints: 1, 5 and 10 years after graduation.";content.append(renderPathways(data,q,(action,base,response)=>reports.open(action,base,response)));return;}
  if(!list.length){const box=element("section",undefined,"empty");box.append(element("h2","No results returned for this view"),element("p","Try another cohort or horizon. The current API does not always distinguish unavailable data from insufficient support."));content.append(box);return;}
  if(state.view==='destinations'&&q.view_mode==='snapshot')content.append(renderDestinationTreemaps(data,q,(action,base,response)=>reports.open(action,base,response)));
  const models=compare?comparisonModels(data,q,options):chartModels(data,state.view,q.view_mode);
  if(compare){const rows=comparisonRows(data,q,options);if(rows.length){const panel=element("section",undefined,"panel comparison-summary");panel.append(element("h2",q.compare_dimension==="major"?"Majors at a glance":"Institutions at a glance"),element("p","Select a name to open its detailed report.","note"),table(rows,q.compare_dimension==="major"?"major_comparison":"school_comparison",data,q,["code","unitid"]));content.append(panel);}else content.append(element("section","No comparison results returned for these selections.","empty"));}
  const overview=list.find(section=>section.key==="overview"&&section.metrics);
  if(overview&&!compare){const panel=element("section",undefined,"panel overview-metrics");panel.append(metricGrid(Object.fromEntries(["alumni","weighted_mean_salary","later_degree_pct","unique_employers"].filter(key=>overview.metrics[key]!=null).map(key=>[key,overview.metrics[key]])),key=>metricReport(key,data,q)));content.append(panel);}
  const results=element("div",undefined,"results chart-results");
  for(const model of models){const panel=element("section",undefined,"panel chart-panel"+((model.type==="lines"&&model.series.length>3||model.type==='bubble')?" full":""));panel.append(element("h2",model.title));if(model.note)panel.append(element("p",model.note,"note"));panel.append(renderChart(model,row=>reports.open(chartAction(model,row,data),compare?comparisonScope(q,model,row):q,data)));results.append(panel);}
  if(state.view==='overview'&&q.include_current_students&&!data.current_student_trend_by_major?.length&&!data.current_student_trend?.length&&!data.major_trend?.current_series?.length){const missing=element('section',undefined,'panel');missing.append(element('h2','Current student projections'),element('p','No current-student projections were returned for this institution, degree and program.','note'));content.append(missing);}
  if(models.length){content.append(element("p","Select a result to open its detailed report.","evidence"),results);}
  const details=element("details",undefined,"data-details");details.open=!models.length;details.append(element("summary",models.length?"View the underlying tables and supporting metrics":"Available data and supporting metrics"));
  const tables=element("div",undefined,"results");for(const section of list){const panel=element("section",undefined,"panel"+(section.metrics?" full":""));panel.append(element("h2",section.key.split(".").map(label).join(" · ")));if(section.metrics)panel.append(metricGrid(section.metrics));else panel.append(table(section.rows,section.key,data,q));tables.append(panel);}details.append(tables);content.append(details);
  const methodology=element("details",undefined,"data-details methodology");methodology.append(element("summary","How the numbers are calculated"),element("p","Alumni counts are calibrated to the graduate population. Salaries follow the outcomes model, and the shaded range shows the 25th to 75th percentiles. Further education reflects the records available. Counts are rounded to whole numbers, salaries to useful amounts, and percentages to at most one decimal. Calculations retain full precision.","evidence"));content.append(methodology);requestAnimationFrame(()=>fitCharts(content));
}
async function load(){
  if(busy)return;const epoch=connectionEpoch;reports.close();let q;try{collect();q=makeQuery(state);}catch(error){$("content").replaceChildren(element("section",error.message,"empty"));setStatus(error.message,true);return;}
  busy=true;controls(true);$("content").setAttribute("aria-busy","true");setStatus("Querying the current protected outcomes API…");
  // Clear prior results so old data cannot appear under newly selected filters.
  $("content").replaceChildren(element("section","Loading this cohort…","empty"));
  try{const data=await api(state.view==="pathways"?"/api/pathways":"/api/dashboard",q);render(data,q);setStatus("Cohort loaded. The selected filters apply to every result below.");}catch(error){if(epoch!==connectionEpoch)return;$("content").replaceChildren(element("section","Results could not be loaded. "+error.message,"empty"));setStatus(error.message,true);}finally{if(epoch===connectionEpoch){busy=false;controls(false);$("content").removeAttribute("aria-busy");}}
}
$("apply").addEventListener("click",load);
for(const button of document.querySelectorAll("[data-mode]"))button.addEventListener("click",async()=>{if(busy)return;state.mode=button.dataset.mode;document.querySelectorAll("[data-mode]").forEach(b=>b.setAttribute("aria-pressed",String(b===button)));if(options)await load();});
function openLogin(){if(busy){cancelPending();setStatus("Pending query cancelled. You can reconnect or explore again.");}$("login-error").textContent="";if(!$("login").open)$("login").showModal();$("token").focus();}
$("connection").addEventListener("click",openLogin);$("welcome-connect").addEventListener("click",openLogin);$("close-dialog").addEventListener("click",()=>$("login").close());
$("login-form").addEventListener("submit",async event=>{
  event.preventDefault();cancelPending();const epoch=connectionEpoch;responseCache.clear();token=$("token").value.trim();$("token").value="";busy=true;controls(true);const submit=$("login-form").querySelector("[type=submit]");submit.disabled=true;$("login-error").textContent="Connecting…";
  try{options=await api("/api/options",{schools:[],degree:state.degree,cip_level:"cip4",majors:[],grad_years:[],horizon:state.horizon,active_tab:"overview"});if(!options.schools?.length)throw new Error("No institutions are available for this credential.");fillOptions(true);controls(false);$("connection").textContent="Connected · manage";$("login").close();setStatus("Connected. Choose a cohort to explore.");}catch(error){if(epoch!==connectionEpoch)return;reports.close();token="";responseCache.clear();options=null;controls(true);$("connection").textContent="Connect data";$("content").innerHTML=welcome;$("welcome-connect").addEventListener("click",openLogin);$("login-error").textContent=error.message;}finally{if(epoch===connectionEpoch){busy=false;submit.disabled=false;}}
  if(epoch===connectionEpoch&&options)await load();
});
$("disconnect").addEventListener("click",()=>{cancelPending();reports.close();token="";responseCache.clear();options=null;state.schools=[];controls(true);$("connection").textContent="Connect data";$("content").innerHTML=welcome;$("welcome-connect").addEventListener("click",openLogin);$("cohort-label").textContent="Connect to choose an institution";$("login").close();setStatus("Disconnected. Your token has been cleared from memory.");});
comparisonControls();updateHeading();
api("/api/health").then(health=>{$("release").textContent=`Current source: ${health.data_version||"version unavailable"} · Exported ${health.data_created_at?health.data_created_at.slice(0,10):"date unavailable"}`;}).catch(()=>{$("release").textContent="Current source metadata could not be reached.";});

}
