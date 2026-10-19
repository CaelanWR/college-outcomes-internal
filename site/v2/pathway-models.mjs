export const CHECKPOINTS=[1,5,10];
const special=role=>['Not observed','Other / limited detail','Other roles'].includes(role);
const valid=v=>typeof v==='number'&&Number.isFinite(v);
export function pathwayModel(data,{mode='checkpoints',pair=[1,5],role='',top=8}={}){
  const raw=data?.pathways?.sequences;
  if(!Array.isArray(raw))return null;
  // Demo maps and reports share a complete-role cohort; the source bundle stays intact.
  const source=raw.filter(r=>valid(r.alumni)&&r.alumni>0&&CHECKPOINTS.every(y=>typeof r[`role_${y}yr`]==='string'&&r[`role_${y}yr`].trim()&&r[`role_${y}yr`].trim()!=='Not observed')).map((r,index)=>({...r,id:index}));
  if(!source.length)return null;
  if(!['checkpoints','next','feeder'].includes(mode)||![[1,5],[5,10]].some(p=>p[0]===pair[0]&&p[1]===pair[1]))throw new Error('Choose a supported pathway perspective and checkpoint pair.');
  const colorRoles=[...new Set(source.flatMap(r=>CHECKPOINTS.map(y=>r[`role_${y}yr`])).filter(name=>name&&!special(name)))].sort();
  const years=mode==='checkpoints'?CHECKPOINTS:pair;
  const focusYear=mode==='checkpoints'?1:mode==='feeder'?pair[1]:pair[0],field=y=>`role_${y}yr`;
  const totals=new Map();for(const r of source){const name=r[field(focusYear)]||'Not observed';if(!special(name))totals.set(name,(totals.get(name)||0)+r.alumni);}
  const roles=[...totals].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0])).map(([name])=>name);
  const selectedRole=mode==='checkpoints'?(roles.includes(role)?role:''):(roles.includes(role)?role:roles[0]||'');
  const records=mode==='checkpoints'&&!selectedRole?source:source.filter(r=>r[field(focusYear)]===selectedRole);
  const total=records.reduce((sum,r)=>sum+r.alumni,0);
  if(!total)return {years,roles,selectedRole,nodes:[],edges:[],records:[],total:0,mode};
  const keep=new Map(years.map(year=>{const counts=new Map();for(const r of records){const name=r[field(year)]||'Not observed';if(!special(name))counts.set(name,(counts.get(name)||0)+r.alumni);}return [year,new Set([...counts].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0])).slice(0,Math.max(1,top)).map(([name])=>name))];}));
  const mapped=r=>years.map(year=>{const label=r[field(year)]||'Not observed';return special(label)||keep.get(year).has(label)?label:'Other roles';});
  const nodes=[],edges=[],nodeMap=new Map(),edgeMap=new Map();
  for(const r of records){const names=mapped(r);years.forEach((year,i)=>{const id=`${year}:${names[i]}`;if(!nodeMap.has(id)){const node={id,year,label:names[i],alumni:0,profiles:0,members:[]};nodes.push(node);nodeMap.set(id,node);}const node=nodeMap.get(id);node.alumni+=r.alumni;node.profiles+=r.profiles||0;node.members.push(r.id);
    if(i<years.length-1){const target=`${years[i+1]}:${names[i+1]}`,eid=`${id}>${target}`;if(!edgeMap.has(eid)){const edge={id:eid,source:id,target,from_year:year,to_year:years[i+1],from_role:names[i],to_role:names[i+1],alumni:0,profiles:0,members:[]};edges.push(edge);edgeMap.set(eid,edge);}const edge=edgeMap.get(eid);edge.alumni+=r.alumni;edge.profiles+=r.profiles||0;edge.members.push(r.id);}
  });}
  nodes.sort((a,b)=>a.year-b.year||Number(special(a.label))-Number(special(b.label))||b.alumni-a.alumni||a.label.localeCompare(b.label));
  for(const item of [...nodes,...edges])item.share_pct=100*item.alumni/total;
  return {mode,years,roles,selectedRole,total,nodes,edges,records,colorRoles,sourceTotal:source.reduce((s,r)=>s+r.alumni,0)};
}
export function pathwayDetail(model,item){
  const members=new Set(item.members);return model.records.filter(r=>members.has(r.id)).sort((a,b)=>b.alumni-a.alumni).map(({id,...r})=>({...r,share_pct:100*r.alumni/item.alumni}));
}
export function pathwayHighlights(model,limit=3){
  if(!model?.records?.length)return [];
  const candidates=model.records.filter(r=>!CHECKPOINTS.some(y=>special(r[`role_${y}yr`]))&&r.role_1yr!==r.role_10yr&&r.alumni>=Math.max(3,model.total*.003)).sort((a,b)=>b.alumni-a.alumni||a.role_1yr.localeCompare(b.role_1yr));
  const seen=new Set(),result=[],family=role=>/educat|teacher/i.test(role)?'education':role;
  for(const r of candidates){if(seen.has(family(r.role_1yr)))continue;seen.add(family(r.role_1yr));result.push({...r});if(result.length>=limit)break;}
  return result;
}
export function pathwaySummary(model){
  const rows=model?.records||[],total=model?.total||0,changed=rows.filter(r=>r.role_1yr!==r.role_10yr).reduce((s,r)=>s+r.alumni,0);
  return {alumni:total,changed_pct:total?100*changed/total:0,destinations:new Set(rows.map(r=>r.role_10yr).filter(role=>role&&!special(role))).size};
}
