import {COLORS} from "./theme.mjs";
import {salaryTrend,salaryDensity} from "./salary-smoothing.mjs";
import {isSalaryKey,normalizeEducation} from "./contracts.mjs";
const numeric=v=>typeof v==="number"&&Number.isFinite(v);
const rows=v=>Array.isArray(v)?v:[];
function field(data,candidates){return candidates.find(key=>data.some(row=>numeric(row[key])));}
function rank(title,data,value,category,note="",color=0){
  if(!value||!category)return null;
  const clean=rows(data).filter(r=>numeric(r[value])&&r[value]>=0&&r[category]!=null).slice().sort((a,b)=>b[value]-a[value]).slice(0,12);
  return clean.length?{type:"bars",title,rows:clean,value,category,note,color:COLORS[color]}:null;
}
export function lineModel(title,data,x,y,group=null,note="",band=false){
  if(!x||!y)return null;
  const clean=rows(data).filter(r=>numeric(r[x]));
  if(!clean.some(r=>numeric(r[y])))return null;
  const names=[...new Set(clean.map(r=>group?String(r[group]??"Selected cohort"):"Selected cohort"))];
  if(group==="title"&&clean.some(r=>numeric(r.n)||numeric(r.alumni)))names.sort((a,b)=>clean.filter(r=>r[group]===b).reduce((n,r)=>n+(r.n||r.alumni||0),0)-clean.filter(r=>r[group]===a).reduce((n,r)=>n+(r.n||r.alumni||0),0));
  const series=names.slice(0,8).map(name=>({name,points:clean.filter(r=>(group?String(r[group]??"Selected cohort"):"Selected cohort")===name).slice().sort((a,b)=>a[x]-b[x])}));
  if(isSalaryKey(y))for(const item of series)item.trend=salaryTrend(item.points,x,y);
  return {type:"lines",title,x,y,series,note,band:band&&series.length===1,smoothing:series.some(s=>s.trend)};
}
export function distributionModel(data){
  const bins=rows(data?.bins).filter(r=>numeric(r.bin_start)&&numeric(r.bin_end)&&r.bin_end>r.bin_start&&numeric(r.n)&&r.n>=0).slice().sort((a,b)=>a.bin_start-b.bin_start);
  const total=bins.reduce((n,r)=>n+r.n,0);
  if(!bins.length||total<=0)return null;
  return {type:"distribution",title:"Salary distribution",value:"share_pct",x:"salary",rows:bins.map(r=>({...r,share_pct:100*r.n/total})),summary:data.summary||{},density:salaryDensity(bins),note:"Salary distribution at the selected horizon."};
}
export function chartModels(data,view,mode){
  data=normalizeEducation(data);
  const models=[]; const add=m=>{if(m)models.push(m);};
  const trajectory=rows(data.career?.earnings);
  if(view==="earnings"&&trajectory.length){const early=trajectory.filter(r=>[1,3,5].includes(r.years_since_grad)&&numeric(r.median_salary));if(early.length)models.push({type:"early",title:"Median salary at 1, 3 and 5 years",rows:early,x:"years_since_grad",value:"median_salary",note:"Median salary and the 25th–75th percentile range."});}
  if(trajectory.length)add(lineModel("Career earnings after graduation",trajectory,"years_since_grad","median_salary","series","Median salary and the 25th–75th percentile range.",true));
  if(view==="earnings"||view==="comparison")add(distributionModel(data.salary_distribution));
  const salary=rows(data.salary_trend_by_school).length?data.salary_trend_by_school:data.salary_trend;
  if((mode==="overtime"||view==="overview")&&rows(salary).length)add(lineModel("Salary by graduation cohort",salary,"grad_year","median_salary",rows(data.salary_trend_by_school).length?"school_name":null,"Same outcome horizon for each cohort. Partial observations use a hollow diamond and a dashed connection."));
  if(rows(data.alumni_trend).length)add(lineModel("Alumni by graduation year",data.alumni_trend,"grad_year","alumni",null,"Alumni by graduation cohort."));
  if(rows(data.alumni_trend_by_school).length)add(lineModel("Alumni cohorts by institution",data.alumni_trend_by_school,"grad_year","alumni","school_name"));
  if(rows(data.major_trend?.series).length)add(lineModel("Top majors over time",data.major_trend.series,"grad_year","share_pct","title","Program share by graduation cohort."));
  if(rows(data.salary_trend_by_major).length&&(mode==="overtime"||view==="overview"))add(lineModel("Program earnings over time",data.salary_trend_by_major,"grad_year","median_salary","title"));
  if(view==='overview'&&rows(data.alumni_trend_by_major).length){
    add(lineModel('Top majors over time',data.alumni_trend_by_major,'grad_year','share_pct','title','Share of graduating alumni by major. Double-click a major to isolate it.'));
    if(!rows(data.alumni_trend).length){const cohorts=new Map();for(const row of data.alumni_trend_by_major)if(numeric(row.total_alumni))cohorts.set(row.grad_year,{grad_year:row.grad_year,alumni:row.total_alumni});add(lineModel('Alumni by graduation year',[...cohorts.values()],'grad_year','alumni',null));}
    add(majorMomentumModel(data));
  }
  if(rows(data.top_majors).length){
    if(view==="overview")add(rank("Top programs in your cohort",data.top_majors,"alumni","title","Alumni by program.",0));
    if(view==="earnings")add(rank("Median salary by program",data.top_majors,"median_salary","title","At the selected outcome horizon.",1));
  }
  if(rows(data.employers?.top).length)add(rank("Top career employers",data.employers.top,"share_pct","employer","Share of alumni by employer.",0));
  if(rows(data.employers?.roles).length)add(rank("Roles within employer",data.employers.roles,"share_pct","role","Within the selected employer.",1));
  for(const [key,title,color] of [["roles","Career destinations",0],["industries","Industry destinations",1]])add(rank(title,data.roles?.[key],"share_pct","label","Share of the selected alumni outcomes.",color));
  const geo=rows(data.geography_all_alumni).length?data.geography_all_alumni:data.geography;
  add(rank("Top alumni locations",geo,"share_pct","location","Share of alumni by location.",0));
  for(const [key,title,color] of [["gender","Gender",0],["race_ethnicity","Race and ethnicity",1]])add(rank(title,data.demographics?.[key],"share_pct","label","Share of alumni by demographic group.",color));
  add(rank("Further education",data.postgrad?.flows,"share_pct","degree_type","Further education recorded in alumni profiles.",0));
  add(rank("Further education destinations",data.postgrad?.schools,field(rows(data.postgrad?.schools),["share_pct","n"]),["school_name","school","label"].find(k=>rows(data.postgrad?.schools).some(r=>typeof r[k]==="string")),"",1));
  if(rows(data.school_comparison).length)add(rank("Median salary across peers",data.school_comparison,"median_salary","school_name","Same program, cohort filters and outcome horizon.",0));
  const seniority=rows(data.career?.seniority);
  if(seniority.length&&field(seniority,["share_pct"]))models.push({type:"stacked",title:"Career stage over time",rows:seniority,x:"years_since_grad",group:"seniority_band",value:"share_pct",note:"Career stage at each year after graduation."});
  for(const [key,title,group] of [["employer_share","Employer pathways over time","employer"],["average_seniority","Average career stage","series"]]){
    const r=rows(data.career?.[key]);add(lineModel(title,r,"years_since_grad",field(r,key==="employer_share"?["share_pct"]:["avg_seniority"]),group));
  }
  for(const [key,title,group] of [["employer_trend","Employer shares by cohort","employer"],["geography_trend","Alumni locations over time","location"],["postgrad_trend","Further education by cohort","degree_type"],["destination_trend","Career destinations over time","label"],["role_trend","Roles over time","label"]]){
    const r=rows(data[key]);add(lineModel(title,r,"grad_year",field(r,["share_pct"]),group));
  }
  const sectionByTitle={"Top programs in your cohort":"top_majors","Median salary by program":"top_majors","Top majors over time":"major_trend.series","Program earnings over time":"salary_trend_by_major","Top career employers":"employers.top","Roles within employer":"employers.roles","Career destinations":"roles.roles","Industry destinations":"roles.industries","Top alumni locations":"geography","Gender":"demographics.gender","Race and ethnicity":"demographics.race_ethnicity","Further education":"postgrad.flows","Further education destinations":"postgrad.schools","Median salary across peers":"school_comparison"};
  for(const model of models)if(sectionByTitle[model.title])model.section=sectionByTitle[model.title];
  const programRows=rows(data.postgrad?.programs);if(programRows.length){const model=rank("Further education programs",programRows,field(programRows,["share_pct","n"]),"label","Click a program to see destination schools.",1);if(model){model.section="postgrad.programs";models.push(model);}}
  if(rows(data.employers?.subroles).length){const model=rank("Detailed roles within employer",data.employers.subroles,"share_pct","role","",1);if(model){model.section="employers.subroles";models.push(model);}}
  if(view==='overview'){for(const model of projectionModels(data))models.push(model);const priority=['Top programs in your cohort','Top career employers','Top majors over time','Projected major mix','Current student projections','Projected student demand by major','Student choice momentum','Major choice momentum and earnings','Program earnings over time','Salary by graduation cohort','Alumni by graduation year','Top alumni locations'];models.sort((a,b)=>(priority.includes(a.title)?priority.indexOf(a.title):99)-(priority.includes(b.title)?priority.indexOf(b.title):99));}
  return models;
}

export function majorMomentumModel(data){
 const trend=rows(data.alumni_trend_by_major),years=[...new Set(trend.map(r=>r.grad_year).filter(numeric))].sort((a,b)=>a-b),recent=years.slice(-3),prior=years.slice(-6,-3);
 if(recent.length!==3||prior.length!==3)return null;
 const share=records=>100*records.reduce((s,r)=>s+r.alumni,0)/records.reduce((s,r)=>s+r.total_alumni,0);
 const clean=rows(data.top_majors).flatMap(major=>{const series=trend.filter(r=>r.code===major.code&&numeric(r.alumni)&&numeric(r.total_alumni)&&r.total_alumni>0),a=series.filter(r=>prior.includes(r.grad_year)),b=series.filter(r=>recent.includes(r.grad_year));if(new Set(a.map(r=>r.grad_year)).size!==3||new Set(b.map(r=>r.grad_year)).size!==3||!numeric(major.median_salary))return [];return [{...major,share_change_pp:share(b)-share(a),prior_share_pct:share(a),recent_share_pct:share(b),recent_alumni:b.reduce((s,r)=>s+r.alumni,0)}];}).sort((a,b)=>b.recent_alumni-a.recent_alumni).slice(0,12);
 return clean.length?{type:'bubble',title:'Major choice momentum and earnings',rows:clean,x:'share_change_pp',y:'median_salary',size:'recent_alumni',section:'top_majors',note:`Change in graduating-major share: ${prior[0]}–${prior.at(-1)} vs. ${recent[0]}–${recent.at(-1)}. Bubble size shows alumni in the recent cohorts; salary uses the selected outcome horizon.`}:null;
}

export function currentStudentRows(data){
 const source=rows(data.current_student_trend_by_major).length?data.current_student_trend_by_major:data.major_trend?.current_series;
 return rows(source).filter(r=>numeric(r.grad_year)&&numeric(r.current_students??r.n)&&(r.current_students??r.n)>0).map(r=>({...r,current_students:r.current_students??r.n,total_current_students:r.total_current_students??r.total_n,projected:true}));
}
export function projectionModels(data){
 const current=currentStudentRows(data),models=[];
 if(!current.length&&!rows(data.current_student_trend).length)return models;
 const totals=new Map(),selected=rows(data.meta?.filters?.majors).length>0;
 if(rows(data.current_student_trend).length){for(const r of data.current_student_trend)if(numeric(r.current_students)&&numeric(r.grad_year))totals.set(r.grad_year,{...r,projected:true});}
 else for(const r of current){if(!totals.has(r.grad_year))totals.set(r.grad_year,{grad_year:r.grad_year,current_students:0,observed_current_profiles:0,projected:true});const t=totals.get(r.grad_year);if(selected){t.current_students+=r.current_students;t.observed_current_profiles+=r.observed_current_profiles||0;}else{t.current_students=Math.max(t.current_students,r.total_current_students||0);t.observed_current_profiles=Math.max(t.observed_current_profiles,r.total_observed_current_profiles||0);}t.current_coverage_pct=t.current_students?Math.min(100,100*t.observed_current_profiles/t.current_students):null;}
 const cohortModel=lineModel('Current student projections',[...totals.values()],'grad_year','current_students',null,'Expected graduating classes from current-student profiles. These counts use the original tracker projection.');if(cohortModel){cohortModel.xLabel='Expected graduation year';models.push(cohortModel);}
 const codes=[...new Set(current.map(r=>r.code))].sort((a,b)=>current.filter(r=>r.code===b).reduce((s,r)=>s+r.current_students,0)-current.filter(r=>r.code===a).reduce((s,r)=>s+r.current_students,0)).slice(0,8),historic=rows(data.alumni_trend_by_major).length?data.alumni_trend_by_major:rows(data.major_trend?.series);
 const series=codes.map(code=>{const c=current.filter(r=>r.code===code).sort((a,b)=>a.grad_year-b.grad_year),h=historic.filter(r=>r.code===code).map(r=>({...r,projected:false})).sort((a,b)=>a.grad_year-b.grad_year);return {name:c[0].title||code,points:[...h,...(h.length?[{grad_year:h.at(-1).grad_year,share_pct:null}]:[]),...c]};});
 if(series.length)models.push({type:'lines',title:'Projected major mix',x:'grad_year',y:'share_pct',series,projection:true,note:'Solid lines show alumni; dotted lines and diamonds show current-student projections. Double-click a major to isolate both.'});
 const demand=new Map();for(const r of current){if(!demand.has(r.code))demand.set(r.code,{code:r.code,title:r.title,current_students:0,observed_current_profiles:0,projected:true});const t=demand.get(r.code);t.current_students+=r.current_students;t.observed_current_profiles+=r.observed_current_profiles||0;}
 const demandModel=rank('Projected student demand by major',[...demand.values()],'current_students','title','Across the expected graduating classes returned by the original tracker.',1);if(demandModel)models.push(demandModel);
 const change=projectedMomentumModel(current);if(change)models.push(change);return models;
}
export function projectedMomentumModel(current){
 const years=[...new Set(current.map(r=>r.grad_year))].sort((a,b)=>a-b),candidates=[];
 const add=ys=>{if(ys.some(y=>!years.includes(y))||candidates.some(c=>c.years.join()===ys.join()))return;const group=current.filter(r=>ys.includes(r.grad_year)),totals=new Map();for(const r of group)if(numeric(r.total_current_students))totals.set(r.grad_year,Math.max(totals.get(r.grad_year)||0,r.total_current_students));const total=[...totals.values()].reduce((a,b)=>a+b,0);if(total>0&&new Set(group.map(r=>r.code)).size>=2)candidates.push({years:ys,start:ys[0],end:ys.at(-1),center:ys.reduce((a,b)=>a+b,0)/ys.length,total,group});};
 add([2024,2025]);add([2028,2029]);years.forEach((year,i)=>{add([year]);if(years[i+1]!==undefined)add([year,years[i+1]]);});
 const choose=(target,after=-Infinity)=>{const possible=candidates.filter(c=>c.start>after),supported=possible.filter(c=>c.total>=25);return (supported.length?supported:possible).sort((a,b)=>Math.abs(a.center-target)-Math.abs(b.center-target)||a.start-b.start)[0];};
 const earlier=choose(2024.5),later=choose(2028.5,earlier?.end);if(!earlier||!later)return null;
 const records=[...new Set([...earlier.group,...later.group].map(r=>r.code))].map(code=>{const a=earlier.group.filter(r=>r.code===code),b=later.group.filter(r=>r.code===code),old=a.reduce((s,r)=>s+r.current_students,0),next=b.reduce((s,r)=>s+r.current_students,0);return {code,title:b[0]?.title||a[0]?.title||code,earlier_projected_students:old,later_projected_students:next,earlier_share_pct:100*old/earlier.total,later_share_pct:100*next/later.total,share_change_pp:100*next/later.total-100*old/earlier.total,observed_current_profiles:b.reduce((s,r)=>s+(r.observed_current_profiles||0),0),projected:true};}).filter(r=>r.later_projected_students>0).sort((a,b)=>b.later_projected_students-a.later_projected_students).slice(0,12);
 return records.length?{type:'bubble',title:'Student choice momentum',rows:records,x:'share_change_pp',y:'later_projected_students',size:'later_projected_students',projection:true,note:`Projected major share change: ${earlier.years.join(' / ')} → ${later.years.join(' / ')}. Bubble size shows projected students in the later classes.`,projectionYears:later.years}:null;
}
