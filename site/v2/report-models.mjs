import {currentStudentRows} from './chart-models.mjs?v=20c03c7369ce';
import {degreeLabel} from './contracts.mjs?v=20c03c7369ce';
const list=value=>Array.isArray(value)?value:[];
const pick=(row,...keys)=>keys.map(k=>row?.[k]).find(v=>v!==null&&v!==undefined&&v!=="");
export function actionFor(section,row,data={}) {
  const common={row};
  if(section==='current.projections')return {kind:'projection',row,rows:[row],title:`${row.title||'Current students'} · Expected class of ${row.grad_year}`};
  if(section==="pathway.sequence")return {...common,kind:"point",title:`${row.role_1yr} → ${row.role_5yr} → ${row.role_10yr}`};
  if(section==='destination.selection')return {...common,kind:'point',title:[row.role_k150_v3||row.role_k50_v3,row.industry_k400||row.industry_k200,row.employer].filter(Boolean).join(' · ')};
  if(section==="postgrad.flows")return {...common,kind:"postgrad-degree",value:degreeLabel(row.degree_type),title:degreeLabel(row.degree_type)};
  if(section==="postgrad.schools")return {...common,kind:"postgrad-school",value:pick(row,"label","school_name","school"),title:pick(row,"label","school_name","school"),degree:data.postgrad?.selected_degree,program:data.postgrad?.selected_program};
  if(section==="postgrad.programs")return {...common,kind:"postgrad-program",value:row.label,title:row.label,degree:data.postgrad?.selected_degree,school:data.postgrad?.selected_school};
  if(section==="employers.top")return {...common,kind:"employer",value:row.employer,title:row.employer};
  if(section==="employers.roles")return {...common,kind:"employer-role",value:row.role,title:row.role,employer:data.employers?.selected_employer};
  if(section==="employers.subroles")return {...common,kind:"point",title:row.role};
  if(section==="demographics.gender"||section==="demographics.race_ethnicity")return {...common,kind:"demographic",dimension:section.split(".")[1],value:row.label,title:row.label};
  if(section==="roles.roles")return {...common,kind:"role",value:row.label,title:row.label};
  if(section==="roles.industries")return {...common,kind:"industry",value:row.label,title:row.label};
  if(section.startsWith("geography"))return {...common,kind:"location",value:row.location,title:row.location};
  if(["top_majors","major_comparison","major_trend.series","salary_trend_by_major"].includes(section)&&row.code)return {...common,kind:"major",value:row.code,title:row.title||row.code};
  if(section==="school_comparison"||section==="salary_trend_by_school")return {...common,kind:"institution",value:pick(row,"unitid","code"),title:pick(row,"school_name","title")};
  if(row.grad_year!=null)return {...common,kind:"cohort",value:row.grad_year,title:`${row.school_name||row.series||""}${row.school_name||row.series?" · ":""}Class of ${row.grad_year}`,school:row.unitid};
  return {...common,kind:"point",title:pick(row,"title","label","role","employer","location","series")||"Chart point"};
}
export function chartAction(model,row,data={}) {
  if(row.projected){const rows=currentStudentRows(data).filter(r=>row.code?r.code===row.code:r.grad_year===row.grad_year).map(({n,total_n,projected,...r})=>r);const {n,total_n,alumni,projected,...record}=row;return {kind:'projection',row:record,rows,title:`Current student projections · ${row.title||(row.code?row.series:null)||(row.grad_year?'Class of '+row.grad_year:'Expected cohorts')}`};}
  if(model.section)return actionFor(model.section,row,data);
  if(model.type==="distribution")return {kind:"point",row,title:"Salary range"};
  if(model.type==="stacked")return {kind:"point",row,title:`${row.seniority_band} · Year ${row.years_since_grad}`};
  if(row.grad_year!=null)return actionFor("cohort",row,data);
  return {kind:"point",row,title:row.years_since_grad!=null?`Year ${row.years_since_grad} after graduation`:model.title};
}
export function reportQuery(base,action) {
  // Reports have their own state; opening a detail never mutates the main cohort.
  const q=structuredClone(base);q.top_n=50;
  const kind=action.kind;
  if(kind==="point"||kind==="pathway"||kind==='treemap'||kind==='projection')return null;
  if(kind.startsWith("postgrad-")){
    q.active_tab="postgrad";q.view_mode="snapshot";
    q.selected_postgrad_degree=kind==="postgrad-degree"?action.value:(action.degree||q.selected_postgrad_degree);
    if(!q.selected_postgrad_degree)throw new Error("Select a further-education degree first.");
    if(kind==="postgrad-degree"){q.selected_postgrad_school=null;q.selected_postgrad_program=null;}
    if(kind==="postgrad-school"){q.selected_postgrad_school=action.value;q.selected_postgrad_program=action.program||null;}
    if(kind==="postgrad-program"){q.selected_postgrad_program=action.value;q.selected_postgrad_school=action.school||q.selected_postgrad_school||null;}
  }else if(kind==="employer"||kind==="employer-role"){
    q.active_tab="employers";q.view_mode="snapshot";
    q.selected_employer=kind==="employer"?action.value:(action.employer||q.selected_employer);
    if(!q.selected_employer)throw new Error("Select an employer first.");
    q.selected_employer_role=kind==="employer-role"?action.value:null;
  }else if(kind==="major"){
    q.compare_mode=false;q.compare_dimension="school";q.majors=[String(action.value)];if(action.row?.grad_year!=null)q.grad_years=[Number(action.row.grad_year)];q.active_tab="overview";q.view_mode="snapshot";
  }else if(kind==="institution"){
    if(!action.value)throw new Error("An institution ID is needed for this report.");
    q.schools=[String(action.value)];if(action.row?.grad_year!=null)q.grad_years=[Number(action.row.grad_year)];q.compare_mode=false;q.compare_dimension="school";q.active_tab="overview";q.view_mode="snapshot";
  }else if(kind==="demographic"){
    if(!["gender","race_ethnicity"].includes(action.dimension))throw new Error("Unsupported demographic selection.");
    q.demographics={...(q.demographics||{}),[action.dimension]:action.value};q.active_tab="overview";q.view_mode="snapshot";
  }else if(kind==="cohort"){
    q.grad_years=[Number(action.value)];if(action.school){q.schools=[String(action.school)];q.compare_mode=false;}q.active_tab="overview";q.view_mode="snapshot";
  }else if(kind==="location"){
    q.active_tab="geography";q.view_mode="overtime";
  }else if(kind==="role"||kind==="industry"){
    q.active_tab="roles";q.view_mode="snapshot";
  }else if(kind==="report"){
    q.active_tab=action.tab||"overview";q.view_mode=action.mode||"snapshot";
  }
  if(q.active_tab==='overview')q.include_current_students=true;
  return q;
}
export function reportGroups(action,data) {
  const groups=[];const add=(title,rows,section)=>{if(list(rows).length)groups.push({title,rows,section});};
  const pg=data.postgrad||{};
  if(action.kind==="postgrad-degree"||action.kind==="report"&&data.postgrad){
    if(action.kind==="report")add("Further-education degrees",pg.flows,"postgrad.flows");
    add(`Destination schools · ${pg.detail_degree||pg.selected_degree||action.value||"Further education"}`,pg.schools,"postgrad.schools");
    add(`Programs · ${pg.detail_degree||pg.selected_degree||action.value||"Further education"}`,pg.programs,"postgrad.programs");
  }else if(action.kind==="postgrad-school"){
    add(`Programs at ${action.value}`,action.program?list(pg.programs).filter(r=>r.label===action.program):pg.programs,"postgrad.programs");
  }else if(action.kind==="postgrad-program"){
    add(`Destination schools for ${action.value}`,pg.schools,"postgrad.schools");
  }else if(action.kind==="employer"){
    add(`Roles at ${action.value}`,data.employers?.roles,"employers.roles");
  }else if(action.kind==="employer-role"){
    add(`Detailed roles at ${data.employers?.selected_employer||action.employer}`,data.employers?.subroles,"employers.subroles");
  }else if(action.kind==="role"){
    add("Roles and industries",list(data.roles?.hierarchy).filter(r=>r.role_k50_v3===action.value),"roles.hierarchy");
    add("Employers and industries",list(data.roles?.employer_hierarchy).filter(r=>r.role_k50_v3===action.value),"roles.employer_hierarchy");
  }else if(action.kind==="industry"){
    const belongs=r=>r.industry_k200===action.value||r.industry_k400===action.value;
    add("Roles within industry",list(data.roles?.hierarchy).filter(belongs),"roles.hierarchy");
    add("Employers within industry",list(data.roles?.employer_hierarchy).filter(belongs),"roles.employer_hierarchy");
  }else if(action.kind==="location"){
    add(`Cohort trend · ${action.value}`,list(data.geography_trend).filter(r=>r.location===action.value),"location.trend");
  }
  return groups;
}
