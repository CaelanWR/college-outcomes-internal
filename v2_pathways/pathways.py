"""Read-only linked role checkpoint aggregates for the v2 workbench."""
from pathlib import Path
import json,sys,argparse
CHECKPOINTS=(1,5,10)
DOCTORATES=['Research Doctorate','PhD','Doctorate','Other Doctorate','Professional Doctorate']
ALIASES={'All Doctorates':DOCTORATES,'Doctorate':DOCTORATES,'PhD':DOCTORATES[:-1],'Research Doctorate':DOCTORATES[:-1],'Professional Doctorate':['Professional Doctorate'],'Other Doctorate':DOCTORATES[:-1]}

def build_pathways(root,filters):
    import duckdb
    root=Path(root).resolve()
    manifest=json.loads((root/'platform_manifest.json').read_text())
    schools=filters.get('schools') or []
    if len(schools)!=1:raise ValueError('Role pathways require one institution. Choose a major within that institution if desired.')
    if filters.get('compare_mode'):raise ValueError('Open one institution or major to explore its role pathways.')
    cip=filters.get('cip_level','cip4')
    if cip not in {'cip2','cip4','cip6'}:raise ValueError('Unsupported program code level.')
    clauses=['person_key IS NOT NULL',"horizon IN ('1yr','5yr','10yr')",'COALESCE(partial_horizon_flag,0)=0']
    params=[str(root/'base_fact/**/*.parquet')]
    def select(column,values):
        if values:clauses.append(column+' IN ('+','.join('?' for _ in values)+')');params.extend(values)
    select('unitid',schools)
    degree=filters.get('degree','Bachelors')
    select('degree',[] if degree=='All' else ALIASES.get(degree,[degree]))
    select(cip,filters.get('majors') or [])
    select('grad_year',filters.get('grad_years') or [])
    for field in ['gender','race_ethnicity']:
        value=(filters.get('demographics') or {}).get(field)
        if value:select(field,[value])
    pg=filters.get('postgrad') or {}
    if pg.get('later_degree_type'):select('later_degree_type',ALIASES.get(pg['later_degree_type'],[pg['later_degree_type']]))
    elif pg.get('no_further_education') is not None:select('no_further_education_flag',[int(pg['no_further_education'])])
    weight="""CASE WHEN degree='Bachelors' AND grad_year>=2023 AND calibration_ipeds_completions IS NULL AND COALESCE(ipeds_calibration_source,'') NOT LIKE 'recent_%' THEN GREATEST(0,COALESCE(position_weight,final_weight,1)) ELSE GREATEST(0,COALESCE(ipeds_calibration_weight,final_weight,1)) END"""
    c=duckdb.connect();c.execute('SET threads=2');c.execute("SET memory_limit='192MB'")
    try:
        c.execute(f"""CREATE TEMP TABLE records AS
          WITH eligible AS (
            SELECT *, {weight} AS cohort_weight,
             CASE WHEN degree='Bachelors' AND horizon='1yr' AND role_k50_v3='Corporate Attorney' THEN NULL
                  WHEN LOWER(TRIM(COALESCE(role_k50_v3,''))) IN ('','unknown','empty') THEN NULL ELSE role_k50_v3 END AS role,
             CASE horizon WHEN '1yr' THEN 1 WHEN '5yr' THEN 5 ELSE 10 END AS checkpoint
            FROM read_parquet(?,union_by_name=true) WHERE {' AND '.join(clauses)}
          ), ranked AS (
            SELECT *,ROW_NUMBER() OVER (PARTITION BY person_key,unitid,degree,cip4,grad_year,checkpoint ORDER BY final_weight DESC NULLS LAST,salary DESC NULLS LAST,role NULLS LAST) AS rk FROM eligible
          ), selected AS (SELECT * FROM ranked WHERE rk=1), linked AS (
            SELECT person_key,unitid,degree,cip4,grad_year,
             FIRST(cohort_weight ORDER BY checkpoint) AS alumni,
             MAX(CASE WHEN checkpoint=1 THEN role END) AS role_1yr,
             MAX(CASE WHEN checkpoint=5 THEN role END) AS role_5yr,
             MAX(CASE WHEN checkpoint=10 THEN role END) AS role_10yr,
             MAX(CASE WHEN checkpoint=1 AND role IS NOT NULL THEN salary END) AS salary_1yr,
             MAX(CASE WHEN checkpoint=5 AND role IS NOT NULL THEN salary END) AS salary_5yr,
             MAX(CASE WHEN checkpoint=10 AND role IS NOT NULL THEN salary END) AS salary_10yr
            FROM selected GROUP BY person_key,unitid,degree,cip4,grad_year
          ) SELECT * FROM linked WHERE alumni>0""",params)
        total,profiles,complete=c.execute("SELECT COALESCE(SUM(alumni),0),COUNT(*),COALESCE(SUM(CASE WHEN role_1yr IS NOT NULL AND role_5yr IS NOT NULL AND role_10yr IS NOT NULL THEN alumni ELSE 0 END),0) FROM records").fetchone()
        threshold=max(0,float(filters.get('_pathway_min_cell_weight',0)))
        salary_threshold=max(threshold,float(manifest.get('suppression_threshold',25)))
        # Publish supported joint sequences; combine limited-detail sequences without inventing role connections.
        result=c.execute("""WITH grouped AS (
          SELECT COALESCE(role_1yr,'Not observed') AS role_1yr,COALESCE(role_5yr,'Not observed') AS role_5yr,COALESCE(role_10yr,'Not observed') AS role_10yr,
           SUM(alumni) AS alumni,COUNT(*) AS profiles,
           MEDIAN(salary_1yr) AS median_salary_1yr,MEDIAN(salary_5yr) AS median_salary_5yr,MEDIAN(salary_10yr) AS median_salary_10yr
          FROM records GROUP BY role_1yr,role_5yr,role_10yr
        ), published AS (
          SELECT CASE WHEN alumni>=? THEN role_1yr ELSE 'Other / limited detail' END AS role_1yr,
                 CASE WHEN alumni>=? THEN role_5yr ELSE 'Other / limited detail' END AS role_5yr,
                 CASE WHEN alumni>=? THEN role_10yr ELSE 'Other / limited detail' END AS role_10yr,
                 alumni,profiles,CASE WHEN alumni>=? THEN median_salary_1yr END AS median_salary_1yr,
                 CASE WHEN alumni>=? THEN median_salary_5yr END AS median_salary_5yr,
                 CASE WHEN alumni>=? THEN median_salary_10yr END AS median_salary_10yr FROM grouped
        ) SELECT role_1yr,role_5yr,role_10yr,SUM(alumni) AS alumni,SUM(profiles) AS profiles,
                 MAX(median_salary_1yr) AS median_salary_1yr,MAX(median_salary_5yr) AS median_salary_5yr,MAX(median_salary_10yr) AS median_salary_10yr
          FROM published GROUP BY role_1yr,role_5yr,role_10yr ORDER BY alumni DESC""",[threshold]*3+[salary_threshold]*3)
        keys=[d[0] for d in result.description];paths=[dict(zip(keys,row)) for row in result.fetchall()]
        known={str(year):c.execute(f'SELECT COALESCE(SUM(CASE WHEN role_{year}yr IS NOT NULL THEN alumni ELSE 0 END),0) FROM records').fetchone()[0] for year in CHECKPOINTS}
        return {'meta':{'data_version':manifest.get('version'),'data_created_at':manifest.get('created_at'),'source_kind':'local-linked-snapshots','source_label':'Local linked snapshot bundle','filters':filters,'checkpoints':list(CHECKPOINTS),'role_taxonomy':'Revelio role_k50_v3','suppression_threshold':threshold,'salary_support_threshold':salary_threshold,'grain':'person, institution, degree, CIP4 and graduation year','definition':'Same graduation records linked across complete checkpoint snapshots. Missing roles do not mean unemployment.'},'pathways':{'available':bool(paths),'alumni':total,'profiles':profiles,'complete_alumni':complete,'observed_by_checkpoint':known,'sequences':paths}}
    finally:c.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);args=parser.parse_args()
    try:print(json.dumps(build_pathways(args.root,json.load(sys.stdin)),allow_nan=False))
    except Exception as exc:print(json.dumps({'error':str(exc)}));sys.exit(1)
