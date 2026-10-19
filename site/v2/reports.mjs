import {label,format,visibleKey,sections,VIEWS,degreeLabel,normalizeEducation} from "./contracts.mjs?v=20c03c7369ce";
import {chartModels,lineModel} from "./chart-models.mjs?v=20c03c7369ce";
import {renderChart,fitCharts} from "./charts.mjs?v=20c03c7369ce";
import {actionFor,chartAction,reportQuery,reportGroups} from "./report-models.mjs?v=20c03c7369ce";
import {projectionNote} from './student-projections.mjs?v=20c03c7369ce';
const el=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
export function createReports({api,getOptions=()=>({})}) {
  const dialog=el("dialog",undefined,"detail-report");dialog.setAttribute("aria-labelledby","detail-title");
  const header=el("header",undefined,"detail-header"),heading=el("div"),eyebrow=el("p","DETAILED REPORT","eyebrow"),title=el("h2");title.id="detail-title";heading.append(eyebrow,title);
  const close=el("button","×","close");close.setAttribute("aria-label","Close detailed report");header.append(heading,close);
  const breadcrumb=el("nav",undefined,"detail-breadcrumb");breadcrumb.setAttribute("aria-label","Report history");
  const context=el("p",undefined,"detail-context"),tabs=el("div",undefined,"detail-tabs"),content=el("div",undefined,"detail-content");content.setAttribute("aria-live","polite");
  dialog.append(header,breadcrumb,context,tabs,content);document.body.append(dialog);
  let history=[],sequence=0;
  close.addEventListener("click",()=>dialog.close());dialog.addEventListener("close",()=>{sequence++;history=[];});
  const current=()=>history.at(-1);
  function metricPanel(record){
    const panel=el("section",undefined,"panel report-metrics"),grid=el("div",undefined,"metrics");
    for(const [key,value] of Object.entries(record||{})){if(typeof value!=="number"||!visibleKey(key)||["grad_year","years_since_grad","bin_start","bin_end"].includes(key))continue;const metric=el("div",undefined,"metric");metric.append(el("small",label(key)),el("strong",format(value,key)));grid.append(metric);}
    if(grid.children.length){panel.append(grid);return panel;}return null;
  }
  function reportTable(rows,section,data,q){
    const keys=[...new Set(rows.flatMap(r=>Object.keys(r)))].filter(k=>visibleKey(k)&&(section!=='pathway.sequence'||rows.some(r=>r[k]!=null&&r[k]!==''))&&!rows.some(r=>r[k]&&typeof r[k]==="object"));
    const wrap=el("div",undefined,"table-wrap"),table=el("table"),head=el("tr"),thead=el("thead");for(const key of keys){const columnLabel=section==='current.projections'?({grad_year:'Expected graduation year',share_pct:'Projected major share',title:'Major',code:'Major code'}[key]||label(key)):key==="label"&&section.startsWith("postgrad.")?({"postgrad.schools":"Destination school","postgrad.programs":"Program"}[section]||label(key)):label(key);const th=el("th",columnLabel);th.scope="col";head.append(th);}thead.append(head);table.append(thead);
    const body=el("tbody");for(const row of rows){const tr=el("tr");const action=actionFor(section,row,data);let linked=false;for(const key of keys){const td=el("td");const value=format(row[key],key);if(section!=="point"&&!linked&&typeof row[key]==="string"&&!["code","unitid"].includes(key)){const button=el("button",value,"report-link");button.setAttribute("aria-label",`View report: ${action.title||value}`);button.addEventListener("click",()=>open(action,q,data));td.append(button);linked=true;}else td.textContent=value;tr.append(td);}body.append(tr);}table.append(body);wrap.append(table);return wrap;
  }
  function updateChrome(entry){
    title.textContent=entry.action.kind==="report"?(VIEWS[entry.q?.active_tab]?.[0]||entry.action.title||"Detailed")+" report":entry.action.title||"Detailed report";
    breadcrumb.replaceChildren();if(history.length>1){const back=el("button","← Back","breadcrumb-link");back.addEventListener("click",()=>{sequence++;history.pop();render(current());});breadcrumb.append(back);}history.forEach((item,index)=>{const button=el("button",item.action.title||"Report","breadcrumb-link");button.disabled=index===history.length-1;button.addEventListener("click",()=>{sequence++;history=history.slice(0,index+1);render(item);});breadcrumb.append(button);if(index<history.length-1)breadcrumb.append(el("span","›"));});
    const q=entry.q||entry.base,options=getOptions()||{},schools=q.schools.map(id=>options.schools?.find(s=>String(s.unitid)===String(id))?.name||id).join(" + ");
    const years=q.grad_years?.length?q.grad_years.join(", "):"All graduation cohorts";
    const horizon=q.pathway_years?`${q.pathway_years.join(", ")} years`:entry.action.kind==="point"&&entry.action.row?.years_since_grad!=null?`${entry.action.row.years_since_grad} years`:(q.horizon==="1yr"?"1 year":q.horizon==="5yr"?"5 years":q.horizon==="10yr"?"10 years":q.horizon);
    context.textContent=`${schools} · ${degreeLabel(q.degree)} · ${years} · ${q.pathway_years?"Role checkpoints: ":q.active_tab==="postgrad"?"Career horizon: ":""}${horizon}${q.active_tab==="postgrad"?"":" after graduation"}${q.selected_postgrad_degree?" · "+q.selected_postgrad_degree:""}${q.selected_postgrad_school?" · "+q.selected_postgrad_school:""}${q.selected_postgrad_program?" · "+q.selected_postgrad_program:""}${q.majors?.length?" · Program "+q.majors.join(", "):""}`;
    if(entry.action.kind==='projection'){const expected=[...new Set((entry.action.rows||[]).map(r=>r.grad_year))].sort((a,b)=>a-b);context.textContent=`${schools} · ${degreeLabel(q.degree)} · Current student projections${expected.length?' · Expected classes '+expected.join(', '):''}`;}
    tabs.replaceChildren();
    if(["major","institution","cohort","demographic","report"].includes(entry.action.kind)){
      for(const view of ["overview","destinations","employers","earnings","career","geography","postgrad"]){const button=el("button",VIEWS[view][0]);button.type="button";button.setAttribute("aria-pressed",String((entry.q?.active_tab||"overview")===view));button.addEventListener("click",async()=>{if(!entry.q)return;entry.q={...entry.q,active_tab:view,view_mode:view==="career"?"overtime":"snapshot"};await fetchEntry(entry);});tabs.append(button);}
    }
  }
  function appendChart(model,data,q){
    const panel=el("section",undefined,"panel chart-panel");panel.append(el("h3",model.title));if(model.note)panel.append(el("p",model.note,"note"));panel.append(renderChart(model,row=>open(chartAction(model,row,data),q,data)));content.append(panel);
  }
  function render(entry){
    updateChrome(entry);content.replaceChildren();const data=normalizeEducation(entry.data||{}),action=entry.action,q=entry.q||entry.base;
    const cohortKinds=["major","institution","cohort","demographic","report"];
    const selected=cohortKinds.includes(action.kind)&&data.overview?data.overview:action.row;
    const metrics=metricPanel(action.kind==='projection'&&selected?Object.fromEntries(Object.entries(selected).map(([key,value])=>[key==='share_pct'?'projected_share_pct':key,value])):selected);if(metrics)content.append(metrics);
    if(action.kind==="pathway"){
      const paths=action.paths||[],panel=el("section",undefined,"panel");panel.append(el("h3","Recorded paths through this selection"),el("p",`${paths.length} linked sequences. Percentages use alumni in this selection.`,"note"),reportTable(paths,"pathway.sequence",data,q));content.append(panel);
    }else if(action.kind==='projection'){
      const panel=el('section',undefined,'panel');panel.append(el('h3','Expected graduating classes'),el('p',projectionNote(data)+' Institution, degree and program filters apply across expected graduation years.','note'),reportTable(action.rows||[],'current.projections',data,q));content.append(panel);
    }else if(action.kind==='treemap'){
      const panel=el('section',undefined,'panel');panel.append(el('h3','Roles, industries and companies in this selection'),el('p','Only the selected destinations are included. Select a row to inspect its detail.','note'),reportTable(action.rows||[],'destination.selection',data,q));content.append(panel);
    }else if(action.kind==="point"){
      const panel=el("section",undefined,"panel");panel.append(el("h3","Point details"),reportTable([action.row||{}],"point",data,q));content.append(panel);
      if(action.row?.employer&&action.row?.years_since_grad!=null){const historyRows=(data.career?.employer_share||[]).filter(r=>r.employer===action.row.employer);const model=lineModel(`Employer history · ${action.row.employer}`,historyRows,"years_since_grad","share_pct","employer");if(model)appendChart(model,data,q);}
      if(action.row?.seniority_band){const historyRows=(data.career?.seniority||[]).filter(r=>r.seniority_band===action.row.seniority_band);const model=lineModel(`Career stage history · ${action.row.seniority_band}`,historyRows,"years_since_grad","share_pct","seniority_band");if(model)appendChart(model,data,q);}
      if(action.row?.years_since_grad!=null){const trajectory=(data.career?.earnings||[]).filter(r=>!action.row.series||r.series===action.row.series);const model=lineModel("Career earnings for the cohort",trajectory,"years_since_grad","median_salary","series","Selected cohort context.",true);if(model)appendChart(model,data,q);}
    }else{
      const groups=reportGroups(action,data);
      if(groups.length){for(const group of groups){const panel=el("section",undefined,"panel report-group");panel.append(el("h3",group.title),el("p",`${group.rows.length} results · Click a name for its report`,"note"),reportTable(group.rows,group.section,data,q));content.append(panel);}}
      else if(cohortKinds.includes(action.kind)){
        const models=chartModels(data,q.active_tab,q.view_mode);for(const model of models)appendChart(model,data,q);
        const detail=el("details",undefined,"data-details");detail.append(el("summary","Supporting tables"));for(const section of sections(data)){const panel=el("section",undefined,"panel");panel.append(el("h3",section.key.split(".").map(label).join(" · ")));if(section.rows)panel.append(reportTable(section.rows,section.key,data,q));else{const m=metricPanel(section.metrics);if(m)panel.append(m);}detail.append(panel);}content.append(detail);
      }else{
        const panel=el("section",undefined,"empty");panel.append(el("h3","Available detail"),el("p","The selected result is shown above. No further breakdown was returned for this selection."));content.append(panel);
      }
    }
    dialog.scrollTop=0;requestAnimationFrame(()=>fitCharts(content));
  }
  async function fetchEntry(entry){
    const request=++sequence;updateChrome(entry);content.replaceChildren(el("section","Loading detailed report…","empty"));tabs.querySelectorAll("button").forEach(b=>b.disabled=true);content.setAttribute("aria-busy","true");dialog.scrollTop=0;
    try{const data=await api("/api/dashboard",entry.q);if(sequence!==request||!dialog.open)return;entry.data=data;render(entry);}catch(error){if(sequence!==request||!dialog.open)return;content.replaceChildren(el("section",error.message,"empty"));const retry=el("button","Retry","primary");retry.addEventListener("click",()=>fetchEntry(entry));content.append(retry);tabs.querySelectorAll("button").forEach(b=>b.disabled=false);}finally{if(sequence===request)content.removeAttribute("aria-busy");}
  }
  async function open(action,base,data={}){
    action={...action,row:action.row?{...action.row}:{}};
    if(action.kind==="institution"&&!action.value)action.value=getOptions()?.schools?.find(s=>s.name===action.title)?.unitid;
    if(action.kind==="cohort"&&action.row?.school_name&&!action.school)action.school=getOptions()?.schools?.find(s=>s.name===action.row.school_name)?.unitid;
    const entry={action,base:structuredClone(base),q:null,data};
    try{entry.q=reportQuery(base,action);}catch(error){action.kind="point";action.note=error.message;}
    if(!dialog.open){history=[];dialog.showModal();}history.push(entry);if(entry.q)await fetchEntry(entry);else render(entry);
  }
  return {open,close:()=>{if(dialog.open)dialog.close();},dialog};
}
