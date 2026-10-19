const rows = value => Array.isArray(value) ? value : [];
const positive = value => typeof value === 'number' && Number.isFinite(value) && value > 0;

export function sourceStudentRows(data) {
  const source = rows(data.current_student_trend_by_major).length
    ? data.current_student_trend_by_major : data.major_trend?.current_series;
  return rows(source).filter(r => Number.isFinite(r.grad_year) && positive(r.current_students ?? r.n))
    .map(r => ({...r, current_students:r.current_students ?? r.n,
      total_current_students:r.total_current_students ?? r.total_n, projected:true}));
}

// This is a steady-size planning estimate, not an enrollment-growth forecast.
// Profile visibility varies by expected graduation year; it must not set class size.
export function prepareStudentProjections(data, baselineData = data) {
  if (data.meta?.projection_model || data.meta?.filters?.compare_mode && data.meta.filters.compare_dimension === 'school') return data;
  const source = sourceStudentRows(data), suppliedTotals = rows(data.current_student_trend);
  const years = [...new Set([...source, ...suppliedTotals].map(r=>r.grad_year).filter(Number.isFinite))].sort((a,b)=>a-b);
  if (!years.length) return data;
  const selected = rows(data.meta?.filters?.majors).length > 0;
  const denominators = new Map();
  for (const r of source) if (positive(r.total_current_students)) denominators.set(r.grad_year, Math.max(denominators.get(r.grad_year)||0,r.total_current_students));
  if (!selected) for (const r of suppliedTotals) if (positive(r.current_students) && !denominators.has(r.grad_year)) denominators.set(r.grad_year,r.current_students);
  const history = new Map();
  for (const r of [...rows(baselineData.alumni_trend_by_major), ...rows(baselineData.major_trend?.series)]) {
    const total = r.total_alumni ?? r.total_n;
    if (r.grad_year < years[0] && !r.partial_horizon && positive(total)) history.set(r.grad_year,total);
  }
  if (!rows(baselineData.meta?.filters?.majors).length) for (const r of rows(baselineData.alumni_trend)) {
    if (r.grad_year < years[0] && !r.partial_horizon && positive(r.alumni) && !history.has(r.grad_year)) history.set(r.grad_year,r.alumni);
  }
  const recent = [...history].sort((a,b)=>a[0]-b[0]).slice(-3);
  const supported = recent.length === 3;
  const baseline = supported ? recent.map(([,n])=>n).sort((a,b)=>a-b)[1] : denominators.get(years[0]);
  // Without a whole-institution denominator, do not inflate a selected major to a school.
  if (!positive(baseline)) return data;
  const model = {method:'steady-size',annual_class_size:baseline,
    baseline_years:supported?recent.map(([year])=>year):[],anchor_year:supported?null:years[0]};
  const project = r => {
    const original = r.current_students ?? r.n, denominator = r.total_current_students ?? r.total_n;
    if (!positive(denominator)) return {...r,projected:true};
    const estimate = baseline * original / denominator;
    return {...r,current_students:estimate,total_current_students:baseline,
      profile_based_students:original,profile_based_total_students:denominator,
      ...(r.n !== undefined ? {n:estimate,total_n:baseline} : {}),
      current_coverage_pct:positive(estimate)?Math.min(100,100*(r.observed_current_profiles||0)/estimate):null,
      total_current_coverage_pct:Math.min(100,100*(r.total_observed_current_profiles||0)/baseline),projected:true};
  };
  const projected = source.map(project);
  const totals = suppliedTotals.length ? suppliedTotals.map(r=>{
    const original = r.current_students, denominator = denominators.get(r.grad_year);
    const count = selected && positive(denominator) ? baseline*original/denominator : !selected ? baseline : original;
    return {...r,current_students:count,profile_based_students:original,
      current_coverage_pct:positive(count)?Math.min(100,100*(r.observed_current_profiles||0)/count):null,projected:true};
  }) : years.map(year=>{
    const group=projected.filter(r=>r.grad_year===year);
    const count=selected?group.reduce((sum,r)=>sum+r.current_students,0):baseline;
    const observed=selected?group.reduce((sum,r)=>sum+(r.observed_current_profiles||0),0):Math.max(0,...group.map(r=>r.total_observed_current_profiles||0));
    return {grad_year:year,current_students:count,observed_current_profiles:observed,
      profile_based_students:selected?group.reduce((sum,r)=>sum+(r.profile_based_students??r.current_students),0):denominators.get(year),
      current_coverage_pct:positive(count)?Math.min(100,100*observed/count):null,projected:true};
  });
  return {...data,meta:{...data.meta,projection_model:model},current_student_trend:totals,
    ...(rows(data.current_student_trend_by_major).length?{current_student_trend_by_major:projected}:{}),
    ...(data.major_trend?{major_trend:{...data.major_trend,current_series:rows(data.major_trend.current_series).map(project)}}:{})};
}

export function projectionNote(data) {
  const model=data.meta?.projection_model;
  if (!model) return 'Current-student profile estimates; class-size history is unavailable.';
  const years=model.baseline_years;
  return years.length ? `Steady-size estimate based on graduating classes ${years[0]}–${years.at(-1)}. Current profiles determine the major mix.`
    : `Steady-size estimate anchored to the nearest expected class (${model.anchor_year}). Current profiles determine the major mix.`;
}
