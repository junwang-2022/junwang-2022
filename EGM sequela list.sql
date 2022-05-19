
use DEV_COMMON.XPI;

select distinct tx_id, tx_name from treatment 
where tx_id not in (select distinct tx_id from ctxa where ctxa_code = 'I'); //3 treatments do not have indication conditions//

-- list of all episodes
create or replace table local_junwang.public.epi as
select cond_id as epi_id, split_part(cond_desc,'-',1) as epi_cat, cond_desc as epi_desc, cond_name as epi_name, 'cd' as epi_type, cond_type, acute_trigger_condition, sgl_2_id, close_period, look_back_period, clear_period, look_forward_period from dev_common.xpi.condition 
union
select tx_id as epi_id, split_part(tx_desc,'-',2) as epi_cat, tx_desc as epi_desc, tx_name as epi_name, 'tx' as epi_type, null as cond_type, null as acute_trigger_condition, sgl_2_id, close_period, look_back_period, clear_period, look_forward_period from dev_common.xpi.treatment
order by epi_type, epi_cat, epi_name;

-- map treatment indication
create or replace table local_junwang.public.epi_index as
select a.*, b.index_tx_cnt
from local_junwang.public.epi a
left join (select cond_id, count(*) as index_tx_cnt from ctxa where ctxa_code = 'I' group by cond_id) b 
on a.epi_id = b.cond_id and a.epi_type = 'cd'
order by epi_type, epi_cat, epi_name;

-- map sequelae
create or replace table local_junwang.public.epi_seq as
select a.*, b.seq_cnt, b.seq_tx_cnt, c.seq_id is not null as as_seq
from local_junwang.public.epi_index a
left join (select epi_type, epi_name, count(distinct seq_id) as seq_cnt, count(distinct seq_tx_id) as seq_tx_cnt from
              (select distinct a.cond_name as epi_name, 'cd' as epi_type, b.cpa_source_2_id as seq_id, c.tx_id as seq_tx_id
              from condition a
              left join cpa b on a.cond_id=b.cpa_source_1_id and b.cpa_code in ('S', 'E')            
              left join ctxa c on b.cpa_source_2_id = c.cond_id and c.ctxa_code = 'I'
              union
              select distinct a.tx_name as epi_name, 'tx' as epi_type, b.cond_id as seq_id, c.tx_id as seq_tx_id
              from treatment a
              left join ctxa b on a.tx_id=b.tx_id and b.ctxa_code = 'S'
              left join ctxa c on b.cond_id = c.cond_id and c.ctxa_code = 'I') 
           group by epi_type, epi_name) b 
on a.epi_type = b.epi_type and a.epi_name = b.epi_name
left join (select cpa_source_2_id as seq_id from cpa where cpa_code in ('S', 'E')            
           union
           select distinct cond_id as seq_id from ctxa b where ctxa_code = 'S') c 
on a.epi_type = 'cd' and a.epi_id = c.seq_id
order by epi_type, epi_cat, epi_name;

-- map if only trigger code
-- create trigger code table
create or replace table local_junwang.public.trigger_code_list as
select tx_id as epi_id, tx_name as epi_name, svc_system as code_system, value_svc as value
from treatment a
left join 
  (select a.*, c.svc_type, c.sgl_2_id, d.sgl_3_id, e.sgl_4_id, f.sgl_5_id from svc_code as a
  left join sgl_1 as b on a.sgl_1_id=b.sgl_1_id
  left join sgl_2 as c on b.sgl_2_id=c.sgl_2_id
  left join sgl_3 as d on c.sgl_3_id=d.sgl_3_id 
  left join sgl_4 as e on d.sgl_4_id=e.sgl_4_id
  left join sgl_5 as f on e.sgl_5_id=f.sgl_5_id) b
on a.sgl_2_id = b.sgl_2_id
union
select cond_id as epi_id, cond_name as epi_name, 'ICD' as code_system, value_dx as value 
from condition a
left join 
  (select a.*, c.chronicity, c.dgl_2_id, d.dgl_3_id, e.dgl_4_id, f.dgl_5_id from dx_code as a
  left join dgl_1 as b on a.dgl_1_id=b.dgl_1_id
  left join dgl_2 as c on b.dgl_2_id=c.dgl_2_id
  left join dgl_3 as d on c.dgl_3_id=d.dgl_3_id 
  left join dgl_4 as e on d.dgl_4_id=e.dgl_4_id
  left join dgl_5 as f on e.dgl_5_id=f.dgl_5_id) b
on a.dgl_2_id = b.dgl_2_id
order by epi_id, epi_name, code_system, value;

create or replace table local_junwang.public.epi_trigger as
select a.*, b.episode_name is null as trigger_only
from local_junwang.public.epi_seq a
left join (select distinct a.episode_name, b.epi_name is null as non_trigger_code from pantry_nci.sndbx.egm_all_codes a
           left join local_junwang.public.trigger_code_list b
           on a.episode_name = b.epi_name and a.code_system = b.code_system and a.value = b.value order by episode_name) b
on a.epi_name = b.episode_name and b.non_trigger_code
order by epi_type, epi_cat, epi_name;

-- map if cost model exists
create or replace table local_junwang.public.epi_cost as
select distinct a.*, c.episode is null as no_cost, d.cond_name as single_seq
from local_junwang.public.epi_trigger a
left join (select distinct cond_tx_id, epi_type,episode from dev_common.xpi.episodes_risk) b
on (a.epi_type = 'cd' and b.epi_type = 'COND' and a.epi_id = b.cond_tx_id) or (a.epi_type = 'tx' and b.epi_type = 'TX' and a.epi_id = b.cond_tx_id)
left join xpi.all_cost_100k_030316 c
on b.episode = c.episode
left join (select distinct a.cpa_source_1_id, b.cond_name from dev_common.xpi.cpa a, dev_common.xpi.condition b where a.cpa_source_2_id = b.cond_id) d
on a.epi_id = d.cpa_source_1_id and a.seq_cnt = 1
order by epi_type, epi_cat, epi_name;

select * from local_junwang.public.epi_cost;


-- pull sequela list
create or replace table local_junwang.public.sequela_list as 
with 
tx as (select distinct a.cond_id, a.cond_name, 'cd' as epi_type, c.tx_name as cond_tx_name, row_number() over(partition by cond_name order by cond_tx_name) as row_num
  from condition a, ctxa b, treatment c
  where a.cond_id = b.cond_id and b.ctxa_code = 'I' and b.tx_id = c.tx_id
  order by cond_name, cond_tx_name),
  
seq as 
(select distinct a.cond_id, a.cond_name, 'cd' as epi_type, c.cond_id as seq_id, c.cond_name as seq_name, row_number() over(partition by a.cond_name order by seq_name) as row_num
  from condition a, cpa b, condition c
  where a.cond_id = b.cpa_source_1_id and b.cpa_code in ('S', 'E') and b.cpa_source_2_id = c.cond_id
  union
  select distinct a.tx_id, a.tx_name, 'tx' as epi_type, c.cond_id as seq_id, c.cond_name as seq_name, row_number() over(partition by tx_name order by seq_name) as row_num
  from treatment a, ctxa b, condition c
  where a.tx_id = b.tx_id and b.ctxa_code = 'S' and b.cond_id = c.cond_id
  order by cond_name, seq_name)  
  

select distinct coalesce(a.cond_id, b.cond_id) as cond_id, coalesce(a.cond_name, b.cond_name) as cond_name, coalesce(a.epi_type, b.epi_type) as epi_type, coalesce(a.row_num, b.row_num) as row_num, a.cond_tx_name, b.seq_name, b.seq_tx_name
from tx a
full outer join
(select distinct a.*, c.tx_name as seq_tx_name from seq a
  left join ctxa b on a.seq_id = b.cond_id and b.ctxa_code = 'I' 
  left join treatment c on b.tx_id = c.tx_id
  order by cond_name, seq_name, seq_tx_name) b
on a.cond_name = b.cond_name and a.row_num = b.row_num
order by epi_type,cond_name, row_num;
  
