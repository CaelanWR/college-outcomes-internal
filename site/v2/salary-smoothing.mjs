// Browser-local, descriptive trend fitting. Source rows and endpoint summaries stay intact.
const valid=v=>typeof v==='number'&&Number.isFinite(v);
const median=a=>{const s=a.slice().sort((x,y)=>x-y),m=Math.floor(s.length/2);return s.length%2?s[m]:(s[m-1]+s[m])/2;};
const clamp=(v,lo,hi)=>Math.max(lo,Math.min(hi,v));
const SUPPORT=['salary_obs','salary_observations','weighted_salary_n','salary_weight'];
export function salarySupport(row){const key=SUPPORT.find(k=>valid(row[k]));return key?Math.max(0,row[key]):null;}
function predictor(points,x,y,span){
  const values=points.map(p=>Math.log1p(p[y])),supports=points.map(salarySupport),known=supports.filter(v=>v!==null&&v>0),typical=known.length?median(known):null;
  // Broad neighborhoods for sparse cohorts; these are display heuristics, not standard errors.
  const fraction=typical===null?.5:typical<10?.7:typical<30?.55:.35;
  const neighbors=Math.min(points.length,Math.max(4,Math.ceil(points.length*fraction)));
  const support= supports.map(n=>n===null||typical===null?1:clamp(Math.sqrt(n/Math.max(1,typical)),.1,4));
  let robust=points.map(()=>1);
  const predict=(at,field=values)=>{
    const distances=points.map(p=>Math.abs(p[x]-at)).sort((a,b)=>a-b);
    const radius=Math.max(span*(typical!==null&&typical<30?.15:.08),distances[neighbors-1]*1.05,1e-6);
    let sw=0,sx=0,sxx=0,sy=0,sxy=0;
    points.forEach((p,i)=>{const u=Math.abs(p[x]-at)/radius;if(u>=1)return;const w=(1-u**3)**3*support[i]*robust[i],dx=p[x]-at;sw+=w;sx+=w*dx;sxx+=w*dx*dx;sy+=w*field[i];sxy+=w*dx*field[i];});
    if(sw<1e-10)return field[points.reduce((best,p,i)=>Math.abs(p[x]-at)<Math.abs(points[best][x]-at)?i:best,0)];
    const determinant=sw*sxx-sx*sx;
    const estimate=determinant>1e-12?(sy*sxx-sxy*sx)/determinant:sy/sw;
    return clamp(estimate,Math.min(...field),Math.max(...field));
  };
  for(let iteration=0;iteration<2;iteration++){
    const residual=points.map((p,i)=>Math.abs(values[i]-predict(p[x]))),scale=median(residual);
    if(scale<.005)break;
    // A floor prevents nearly exact fits from treating modest changes as outliers.
    const cutoff=Math.max(.09,6*scale);robust=residual.map(r=>Math.max(.05,(1-clamp(r/cutoff,0,1)**2)**2));
  }
  const lower=points.every(p=>valid(p.p25_salary)&&p.p25_salary>=0)?points.map(p=>Math.log1p(p.p25_salary)):null;
  const upper=points.every(p=>valid(p.p75_salary)&&p.p75_salary>=0)?points.map(p=>Math.log1p(p.p75_salary)):null;
  return {typical,fraction,at:at=>{const value=Math.expm1(predict(at));return {[x]:at,[y]:value,...(lower&&upper?{p25_salary:Math.min(value,Math.expm1(predict(at,lower))),p75_salary:Math.max(value,Math.expm1(predict(at,upper)))}:{})};}};
}
export function salaryTrend(points,x,y,{maxGap=2}={}){
  // Missing years, explicit nulls, and partial-window boundaries never train across each other.
  const segments=[];let segment=[];
  const flush=()=>{if(segment.length)segments.push(segment);segment=[];};
  for(const row of points.slice().sort((a,b)=>a[x]-b[x])){
    if(!valid(row[x])||!valid(row[y])||row[y]<0||salarySupport(row)===0){flush();continue;}
    const last=segment.at(-1);
    if(last&&(row[x]<=last[x]||row[x]-last[x]>maxGap||Boolean(row.partial_horizon)!==Boolean(last.partial_horizon)))flush();
    segment.push(row);
  }flush();
  let fitted=0;
  const result=segments.map(rows=>{
    if(rows.length<3)return {fitted:false,partial:Boolean(rows[0].partial_horizon),points:rows.map(r=>({...r})),atObservations:rows.map(r=>({...r}))};
    const min=rows[0][x],max=rows.at(-1)[x],fit=predictor(rows,x,y,max-min),count=Math.min(300,Math.max(24,Math.ceil((max-min)*12)));
    fitted++;return {fitted:true,partial:Boolean(rows[0].partial_horizon),support:fit.typical,span:fit.fraction,points:Array.from({length:count+1},(_,i)=>fit.at(min+(max-min)*i/count)),atObservations:rows.map(r=>fit.at(r[x]))};
  });
  return fitted?{method:'robust-local-linear-log',segments:result}:null;
}

export function salaryDensity(bins){
  const rows=bins.filter(r=>valid(r.bin_start)&&valid(r.bin_end)&&r.bin_end>r.bin_start&&valid(r.n)&&r.n>=0),total=rows.reduce((s,r)=>s+r.n,0);
  if(rows.length<3||total<=0)return null;
  const min=Math.min(...rows.map(r=>r.bin_start)),max=Math.max(...rows.map(r=>r.bin_end)),width=median(rows.map(r=>r.bin_end-r.bin_start));
  const samples=rows.map(r=>({x:(r.bin_start+r.bin_end)/2,mass:r.n/total})),mean=samples.reduce((s,p)=>s+p.x*p.mass,0),sd=Math.sqrt(samples.reduce((s,p)=>s+(p.x-mean)**2*p.mass,0));
  // Total support only guides display bandwidth; binned counts cannot establish effective sample size.
  const bandwidth=Math.max(width*(total<30?2:1.25),1.06*sd*Math.max(3,total)**(-.2));
  const gaussian=z=>Math.exp(-.5*z*z)/Math.sqrt(2*Math.PI);
  const points=Array.from({length:241},(_,i)=>{const x=min+(max-min)*i/240;return {salary:x,density:samples.reduce((s,p)=>s+p.mass*gaussian((x-p.x)/bandwidth)/bandwidth,0)};});
  let area=0;for(let i=1;i<points.length;i++)area+=(points[i].salary-points[i-1].salary)*(points[i].density+points[i-1].density)/2;
  if(!valid(area)||area<=0)return null;
  return {method:'binned-gaussian-density',bandwidth,referenceWidth:width,points:points.map(p=>({salary:p.salary,share_pct:100*p.density/area*width}))};
}
