import {lineModel,distributionModel,projectionModels} from './chart-models.mjs?v=34e7f57be8d1';
import {COLORS} from './theme.mjs?v=34e7f57be8d1';
import {sections} from './contracts.mjs?v=34e7f57be8d1';
const list=v=>Array.isArray(v)?v:[];
const valid=v=>typeof v==='number'&&Number.isFinite(v);
const identity=r=>String(r.unitid??r.code??'');
export function comparisonRows(data,q,options={}){
  const major=q.compare_dimension==='major',raw=list(data[major?'major_comparison':'school_comparison']),lookup=new Map(raw.map(r=>[identity(r),r]));
  return (major?q.majors:q.schools).map(code=>{
    const r=lookup.get(String(code))||{},title=r.title||r.school_name||(major?options.major_options?.find(m=>String(m.code)===String(code))?.title:options.schools?.find(s=>String(s.unitid)===String(code))?.name)||String(code);
    const row={title,code:String(code),...(major?{}:{unitid:String(code)})};
    // The legacy Career entity list sums annual counts. It is not a unique alumni denominator.
    if(q.active_tab==='career'){
      for(const year of [1,5,10])row[`median_salary_${year}yr`]=list(data.career?.earnings).find(p=>p.series===title&&p.years_since_grad===year)?.median_salary??null;
    }else for(const key of ['alumni','salary_weight','weighted_mean_salary','median_salary','later_degree_pct','unique_employers'])row[key]=r[key]??null;
    return row;
  });
}
export function comparisonModels(data,q,options={}){
  const entities=comparisonRows(data,q,options),models=[],major=q.compare_dimension==='major',section=major?'major_comparison':'school_comparison',colors=Object.fromEntries(entities.map((r,i)=>[r.title,COLORS[i]]));
  const add=model=>{if(model){model.entities=entities;if(model.series){for(const series of model.series)series.color=colors[series.name];model.series.sort((a,b)=>entities.findIndex(r=>r.title===a.name)-entities.findIndex(r=>r.title===b.name));}models.push(model);}};
  const bar=(title,value)=>{const rows=entities.filter(r=>valid(r[value]));if(rows.length)add({type:'bars',title,rows,value,category:'title',colors,color:COLORS[0],section,note:'Same degree, graduation cohorts and outcome horizon.'});};
  if(q.active_tab!=='career'){bar('Median salary comparison','median_salary');bar('Further education comparison','later_degree_pct');}
  add(lineModel('Career earnings comparison',data.career?.earnings,'years_since_grad','median_salary','series','Smoothed salary trajectories for each selection.'));
  if(q.active_tab==='earnings'&&list(data.career?.earnings).length)add({type:'early',title:'Median salary at 1, 3 and 5 years',rows:list(data.career.earnings).filter(r=>[1,3,5].includes(r.years_since_grad)&&valid(r.median_salary)).sort((a,b)=>entities.findIndex(r=>r.title===a.series)-entities.findIndex(r=>r.title===b.series)),x:'years_since_grad',value:'median_salary',colors,note:'Observed medians and 25th–75th percentile salary ranges.'});
  if(q.view_mode==='overtime'){
    const salary=major?data.salary_trend_by_major:data.salary_trend_by_school,alumni=major?data.alumni_trend_by_major:data.alumni_trend_by_school;
    const trend=lineModel('Salary by graduation cohort',salary,'grad_year','median_salary',major?'title':'school_name','The same outcome horizon for every cohort. Partial windows remain separate.');if(trend)trend.section=major?'salary_trend_by_major':'salary_trend_by_school';add(trend);
    const counts=lineModel('Alumni by graduation cohort',alumni,'grad_year','alumni',major?'title':'school_name');if(counts)counts.section=major?'major_comparison':'school_comparison';add(counts);
  }
  if(major&&q.active_tab==='overview')for(const model of projectionModels(data).filter(m=>['Projected major mix','Projected student demand by major'].includes(m.title)))add(model);
  if(q.active_tab==='career')add(lineModel('Average career stage comparison',data.career?.average_seniority,'years_since_grad','avg_seniority','series'));
  const distributions=data.salary_distributions_by_entity||{},distributionModels=[];
  for(const entity of entities){const bins=list(distributions.bins).filter(r=>String(r.code)===entity.code),summary=list(distributions.summary).find(r=>String(r.code)===entity.code);const model=distributionModel({bins,summary});if(!model)continue;model.title=`Salary distribution · ${entity.title}`;model.entity=entity;
    if(model.density){const factor=5000/model.density.referenceWidth;model.density={...model.density,referenceWidth:5000,points:model.density.points.map(p=>({...p,share_pct:p.share_pct*factor}))};}
    model.note='Same salary and percentage scales for each selection.';distributionModels.push(model);
  }
  if(distributionModels.length){const min=Math.min(...distributionModels.flatMap(m=>m.rows.map(r=>r.bin_start))),max=Math.max(...distributionModels.flatMap(m=>m.rows.map(r=>r.bin_end))),top=Math.max(...distributionModels.flatMap(m=>[...m.rows.map(r=>r.share_pct),...(m.density?.points||[]).map(r=>r.share_pct),...m.rows.map(r=>r.share_pct*(m.density?.referenceWidth||5000)/(r.bin_end-r.bin_start))]))*1.14;for(const model of distributionModels){model.domain={min,max,top};add(model);}}
  return models;
}
export function comparisonScope(q,model,row){
  const entity=model.entity||model.entities?.find(r=>r.code===String(row.unitid??row.code??'')||r.title===row.series||r.title===row.school_name||r.title===row.title);
  if(!entity)return q;
  const scoped={...q,compare_mode:false,compare_dimension:"school"};if(q.compare_dimension==='major')scoped.majors=[entity.code];else scoped.schools=[entity.code];return scoped;
}

export function comparisonSections(data,q){return sections(data).filter(s=>s.key!=="overview"&&s.key!=="salary_trend"&&s.key!=="salary_distribution"&&!s.key.startsWith("salary_distribution.")&&!(q.active_tab==="career"&&["major_comparison","school_comparison"].includes(s.key)));}
