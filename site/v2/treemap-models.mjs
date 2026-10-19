export const DESTINATION_TYPES={roles:{title:'Roles',levels:['role_k50_v3','role_k150_v3']},industries:{title:'Industries',levels:['industry_k200','industry_k400']},companies:{title:'Companies',levels:['employer']}};
const number=v=>typeof v==='number'&&Number.isFinite(v);
export function destinationRows(data,types=['roles','companies']){
 const raw=types.includes('companies')?data.roles?.employer_hierarchy:data.roles?.hierarchy;
 return (Array.isArray(raw)?raw:[]).filter(r=>number(r.n)&&r.n>0&&types.every(type=>typeof r[DESTINATION_TYPES[type].levels[0]]==='string'&&r[DESTINATION_TYPES[type].levels[0]].trim()));
}
export function filterDestinations(rows,type,path=[]){
 const levels=DESTINATION_TYPES[type]?.levels;if(!levels)throw new Error('Choose roles, industries or companies.');
 return rows.filter(r=>path.every((name,index)=>String(r[levels[index]]||r[levels[0]]||'').trim()===name));
}
export function destinationNodes(rows,type,path=[]){
 const config=DESTINATION_TYPES[type],selected=filterDestinations(rows,type,path),depth=Math.min(path.length,config.levels.length-1),field=config.levels[depth],nodes=new Map();
 for(const r of selected){const name=String(r[field]||r[config.levels[0]]||'').trim();if(!name)continue;if(!nodes.has(name))nodes.set(name,{label:name,n:0,salarySum:0,salarySupport:0,rows:[],drillable:path.length<config.levels.length});const node=nodes.get(name);node.n+=r.n;node.rows.push(r);if(number(r.weighted_mean_salary)&&number(r.salary_sum)&&number(r.salary_weight)&&r.salary_weight>0){node.salarySum+=r.salary_sum;node.salarySupport+=r.salary_weight;}}
 const total=[...nodes.values()].reduce((s,n)=>s+n.n,0);
 return [...nodes.values()].map(({salarySum,salarySupport,...n})=>({...n,share_pct:total?100*n.n/total:0,mean_salary:salarySupport?salarySum/salarySupport:null})).sort((a,b)=>b.n-a.n||a.label.localeCompare(b.label));
}
// Squarified area layout; tile area remains proportional to the original counts.
export function treemapLayout(nodes,width=600,height=430){
 const total=nodes.reduce((s,n)=>s+n.n,0);if(!total||width<=0||height<=0)return [];
 const pending=nodes.map(n=>({...n,area:n.n/total*width*height})),result=[];let x=0,y=0,w=width,h=height,row=[];
 const worst=(list,side)=>{if(!list.length)return Infinity;const areas=list.map(n=>n.area),sum=areas.reduce((a,b)=>a+b,0);return Math.max(side*side*Math.max(...areas)/(sum*sum),sum*sum/(side*side*Math.min(...areas)));};
 const place=()=>{const sum=row.reduce((s,n)=>s+n.area,0);if(w>=h){const strip=sum/h;let offset=y;for(const n of row){const size=n.area/strip;result.push({...n,x,y:offset,width:strip,height:size});offset+=size;}x+=strip;w=Math.max(0,w-strip);}else{const strip=sum/w;let offset=x;for(const n of row){const size=n.area/strip;result.push({...n,x:offset,y,width:size,height:strip});offset+=size;}y+=strip;h=Math.max(0,h-strip);}row=[];};
 while(pending.length){const next=pending[0],side=Math.min(w,h);if(!row.length||worst([...row,next],side)<=worst(row,side)){row.push(pending.shift());}else place();}if(row.length)place();return result;
}
