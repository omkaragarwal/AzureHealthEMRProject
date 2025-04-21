--Total Charge Amount per provider by department

select
  concat(p.FirstName,' ',p.LastName) as ProviderName,
  d.SRC_Dept_ID,
  sum(t.PaidAmount) as TotalChargeAmount
from gold.fact_transactions t left join gold.provider p  on t.FK_providerID = p.provider_id
left join gold.dim_dept d on t.FK_DeptID = d.SRC_Dept_id
group by 1,2;

--Total Charge Amount per provider by department for each month for year 2024

select
  concat(p.FirstName,' ',p.LastName) as ProviderName,
  d.NAME,
  date_format(ft.ServiceDate,'yyyyMM') yyyymm,
  sum(ft.Amount) as Total_Charge_Amount,
  sum(ft.PaidAmount) as Total_Paid_Amount
from
  gold.fact_transactions ft left join gold.provider p  on ft.FK_providerID = p.provider_id
  left join gold.dim_dept d on ft.FK_DeptID = d.SRC_Dept_id
where year(ft.ServiceDate) = 2024
group by 1,2,3
order by 1,3
