export const VIEWS = {
  overview: ["Institution overview", "Understand your cohort and its outcomes."],
  destinations: ["Career destinations", "Explore the roles and industries your alumni enter."],
  employers: ["Employer network", "See where your alumni work and the roles they hold."],
  pathways: ["Role pathways", "Follow linked alumni roles and explore feeder and next-checkpoint outcomes."],
  career: ["Career progression", "Follow earnings, seniority and employer patterns over a career."],
  earnings: ["Earnings", "Inspect earnings at a consistent outcome horizon."],
  geography: ["Regional impact", "Explore the locations recorded in alumni outcomes."],
  postgrad: ["Further education", "Understand observed subsequent degrees and education destinations."],
  demographics: ["Demographics", "Inspect the demographic fields supplied by the existing model."],
  comparison: ["Compare outcomes", "Compare majors or peer institutions using the same degree, cohort and horizon."],
  coverage: ["Evidence & coverage", "Inspect the support available for your selected cohort."]
};
export function defaultSchool(schools=[]) {
  return String(schools.find(s=>String(s.unitid)==='243744')?.unitid ?? schools[0]?.unitid ?? '');
}
export function makeQuery(state,{optionsOnly=false}={}) {
  if (!(state.view in VIEWS)) throw new Error("Choose a valid view.");
  if (!state.schools?.length) throw new Error("Select at least one institution.");
  if (state.schools.length > 3) throw new Error("Choose up to three comparison institutions.");
  if(new Set(state.schools).size!==state.schools.length)throw new Error("Choose distinct institutions.");
  const compare=state.view==="comparison",dimension=state.compareDimension||"school";
  if(!["major","school"].includes(dimension))throw new Error("Choose majors or peer institutions to compare.");
  const majors=compare&&dimension==="major"?[...(state.majors||[])]:state.major?[state.major]:[];
  if(compare&&!optionsOnly){
    if(dimension==="school"&&state.schools.length<2)throw new Error("Select two or three institutions for a peer comparison.");
    if(dimension==="major"&&(majors.length<2||majors.length>3))throw new Error("Select two or three majors to compare.");
    if(dimension==="major"&&state.schools.length!==1)throw new Error("Compare majors within one institution.");
    if(new Set(majors).size!==majors.length)throw new Error("Choose distinct majors.");
  }
  const tab=compare?(state.comparisonTab||"overview"):state.view;
  if(compare&&!["overview","earnings","career"].includes(tab))throw new Error("Choose a valid comparison analysis.");
  return {
    schools: [...state.schools], degree: state.degree || "Bachelors", cip_level: state.cipLevel || "cip4",
    majors: optionsOnly?[]:majors, grad_years: optionsOnly?[]:[...(state.years || [])],
    horizon: state.horizon || "1yr", demographics: {}, postgrad: {},
    include_current_students: tab==="overview", include_school_employers: false,
    compare_mode: compare, compare_dimension: compare?dimension:"school",
    active_tab: optionsOnly?"overview":state.view==="pathways"?"career":tab,
    view_mode: state.view==="pathways"?"snapshot":state.mode || "snapshot", top_n: 12
  };
}
export function degreeLabel(value){
 const name=String(value??'');return /^(?:Ph\.?D\.?|Doctorate|Research Doctorate|Other Doctorate)$/i.test(name.trim())?'PhD':name;
}
export function normalizeEducation(data){
 if(!data||typeof data!=='object')return data;
 const merge=rows=>{if(!Array.isArray(rows))return rows;const groups=new Map();for(const raw of rows){const row={...raw,degree_type:degreeLabel(raw.degree_type)},identity=JSON.stringify(Object.fromEntries(Object.entries(row).filter(([key])=>!['n','n_alumni','alumni','share_pct'].includes(key))));if(!groups.has(identity)){groups.set(identity,row);continue;}const prior=groups.get(identity);for(const key of ['n','n_alumni','alumni','share_pct'])if(typeof row[key]==='number')prior[key]=(prior[key]||0)+row[key];}return [...groups.values()];};
 const result={...data};if(data.postgrad){result.postgrad={...data.postgrad,flows:merge(data.postgrad.flows)};for(const key of ['selected_degree','detail_degree'])if(data.postgrad[key]!=null)result.postgrad[key]=degreeLabel(data.postgrad[key]);}
 if(data.postgrad_trend)result.postgrad_trend=merge(data.postgrad_trend);
 return result;
}
export const LABELS = {
  profile_based_students:'Profile-based estimate', profile_based_total_students:'Cohort profile-based estimate',
  projected_share_pct: "Projected major share", current_students: "Projected students", total_current_students: "Projected cohort size", observed_current_profiles: "Observed student profiles", total_observed_current_profiles: "Observed student profiles in cohort", current_coverage_pct: "Profile coverage", total_current_coverage_pct: "Cohort profile coverage", earlier_projected_students: "Earlier projected students", later_projected_students: "Later projected students",
  share_change_pp: 'Change in major share', recent_alumni: 'Alumni in recent cohorts', prior_share_pct: 'Earlier major share', recent_share_pct: 'Recent major share', role_k50_v3: 'Role', role_k150_v3: 'Detailed role', industry_k200: 'Industry', industry_k400: 'Detailed industry', employer: 'Company',
  profiles: "Profiles", role_1yr: "Role · year 1", role_5yr: "Role · year 5", role_10yr: "Role · year 10",
  alumni: "Alumni", n: "Alumni", n_alumni: "Alumni", weighted_alumni: "Alumni",
  raw_n: "Source records", observed_profiles: "Profiles", observed_current_profiles: "Student profiles",
  salary_weight: "Alumni with salary data", salary_obs: "Salary records", weighted_salary_n: "Alumni with salary data",
  median_salary_1yr: "Median salary · year 1", median_salary_5yr: "Median salary · year 5", median_salary_10yr: "Median salary · year 10", weighted_mean_salary: "Average salary", median_salary: "Median salary",
  weighted_mean_starting_salary: "Average starting salary", mean_salary: "Average salary",
  years_since_grad: "Years after graduation", grad_year: "Graduation cohort", years_out: "Years after graduation", years_after_degree: "Years after degree",
  later_degree_n: "Alumni with further education", no_further_n: "Alumni without further education", later_degree_pct: "Further education", share_pct: "Share of alumni", unique_employers: "Employers",
  unique_locations: "Locations", school_name: "Institution", unitid: "Institution ID",
  bin_start: "Salary from", bin_end: "Salary to", p10: "10th percentile", p25: "25th percentile", median: "Median salary", p75: "75th percentile", p90: "90th percentile"
};
export function label(key) { return LABELS[key] || key.replace(/weighted_?/gi, "").replaceAll("_", " ").replace(/^./, c => c.toUpperCase()); }
export function visibleKey(key) {
  return !["projected", "profile_weight", "final_weight", "position_weight", "ipeds_calibration_weight", "raw_rows", "salary_sum"].includes(key)
    && !(key.endsWith("_weight") && key !== "salary_weight");
}
export function isSalaryKey(key) {
  if (/salary_(weight|obs|observations|n|count)|salary_n|salary_weight|salary_coverage|salary.*_pct|salary.*growth/i.test(key)) return false;
  return /salary|earnings|wage|(^|_)pay($|_)|^p(10|25|50|75|90)$|^median$|^bin_(start|end)$/i.test(key);
}
export function format(value, key="") {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "—";
    if (isSalaryKey(key)) {
      const absolute=Math.abs(value), sign=value<0?"−":"";
      if (absolute>=999500) return sign+"$"+(absolute/1000000).toLocaleString("en-US",{maximumFractionDigits:1})+"m";
      if (absolute>=10000) return sign+"$"+Math.round(absolute/1000).toLocaleString("en-US")+"k";
      const step=absolute>=1000?100:1;
      return sign+"$"+(Math.round(absolute/step)*step).toLocaleString("en-US");
    }
    if(key.endsWith("_pp"))return value.toLocaleString("en-US",{maximumFractionDigits:1})+" pp";
    if (key.endsWith("_pct")) {
      const rounded=Math.round(value*10)/10;
      if(value!==0&&rounded===0)return value<0?">−0.1%":"<0.1%";
      return rounded.toLocaleString("en-US", {maximumFractionDigits:1}) + "%";
    }
    if (key === "grad_year" || key === "year" || key.endsWith("_year") || key === "unitid") return String(Math.round(value));
    if (/^(avg|average|mean)_|ratio|probability|confidence|tenure|duration/i.test(key)) return value.toLocaleString("en-US", {maximumFractionDigits:1});
    return Math.round(value).toLocaleString("en-US");
  }
  return degreeLabel(value);
}
export function formatAxis(value,key="") {
  if(!Number.isFinite(value))return "—";
  if(!isSalaryKey(key)&&!key.endsWith("_pct")&&!/year|unitid|avg|seniority|duration|tenure/.test(key)&&Math.abs(value)>=10000){
    const divisor=Math.abs(value)>=1000000?1000000:1000,suffix=divisor===1000000?"m":"k";
    return (value/divisor).toLocaleString("en-US",{maximumFractionDigits:1})+suffix;
  }
  return format(value,key);
}
export function sections(data) {
  const result=[];
  function walk(value, path) {
    if (path === "meta" || value === null || value === undefined) return;
    if (Array.isArray(value)) {
      if (value.length && value.every(row => row && typeof row === "object" && !Array.isArray(row))) result.push({key:path, rows:value});
      return;
    }
    if (typeof value !== "object") return;
    const scalar = Object.fromEntries(Object.entries(value).filter(([,v]) => v !== null && typeof v !== "object"));
    if (Object.keys(scalar).some(k => typeof scalar[k] === "number")) result.push({key:path, metrics:scalar});
    for (const [key,child] of Object.entries(value)) if (child && typeof child === "object") walk(child,path ? path + "." + key : key);
  }
  walk(data, ""); return result;
}
