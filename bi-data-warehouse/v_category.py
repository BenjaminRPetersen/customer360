# -*- coding: utf-8 -*-
"""
Title: Category Subject Area Redshift

Created on Wed Dec 29↔

@author: benjaminrpetersen
"""

"""
Import Packages, Tools and Authorizations
"""
import os, datetime # Standard Packages
os.chdir(os.path.dirname(os.path.realpath(__file__)))
parent_path = os.path.dirname(os.getcwd())
os.chdir(parent_path+'/authorizations/') # System Authorizations
from redshift import conn
start = datetime.datetime.now()
          
"""
Update data in Redshift
"""
cur = conn.raw_connection().cursor()
cur.execute("""create or replace view customer360.v_category as 
            select 	a.category_key,
          a.category_description,
		a.full_date::date,
		coalesce(sum(adv.extended_price),0)::bigint as total_advantage_spend,
		coalesce(sum(adv_ly.extended_price),0)::bigint as total_advantage_spend_ly,
		coalesce(sum(e.ebuy_rfqs_opened),0)::bigint as rfqs_opened,
		coalesce(sum(e_ly.ebuy_rfqs_opened),0)::bigint as rfqs_opened_ly,
		coalesce(sum(opp.opportunities),0)::bigint as opportunities,
		coalesce(sum(opp.opportunity_amount),0)::bigint as opportunity_size,
		coalesce(sum(opp_ly.opportunities),0)::bigint as opportunities_ly,
		coalesce(sum(opp_ly.opportunity_amount),0)::bigint as opportunity_size_ly,
		coalesce(sum(camp.campaign_members),0)::bigint as campaign_members,
		coalesce(sum(camp.clp_credits_earned),0)::bigint as clp_credits_earned,
		coalesce(sum(camp_ly.campaign_members),0)::bigint as campaign_members_ly,
		coalesce(sum(camp_ly.clp_credits_earned),0)::bigint as clp_credits_earned_ly,
		coalesce(sum(fpds.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated,
		coalesce(sum(fpds_ly.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated_ly
from 	(customer360.d_category
		cross join (select * from customer360.d_date where full_date between current_date-365 and current_date)) a --All Customers and Dates
		left join	--ADVANTAGE METRICS
			( 	select category_key, purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by category_key, purchase_date_key) adv
			on adv.category_key = a.category_key
			and a.date_key = adv.purchase_date_key
		left join	--ADVANTAGE METRICS
			( 	select category_key, purchase_date_key+31536000 as purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by category_key, purchase_date_key+31536000) adv_ly
			on adv_ly.category_key = a.category_key
			and a.date_key = adv_ly.purchase_date_key
		left join (	select category_key, issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy_category
					group by category_key, issue_day_key) e
			on a.category_key = e.category_key
			and a.date_key = e.issue_day_key
		left join (	select category_key, issue_day_key+31536000 as issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy_category
					group by category_key, issue_day_key+31536000) e_ly
			on a.category_key = e_ly.category_key
			and a.date_key = e_ly.issue_day_key
		left join (
					select category_key,created_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity_category
					group by category_key,created_date_key) opp
			on a.category_key = opp.category_key
			and a.date_key = opp.created_date_key
		left join (
					select category_key,created_date_key+31536000 as created_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity_category
					group by category_key,created_date_key+31536000) opp_ly
			on a.category_key = opp_ly.category_key
			and a.date_key = opp_ly.created_date_key
		left join (select category_key, created_date_key, sum(total_clp_credits) as clp_credits_earned, sum(campaign_leads + campaign_contacts) as campaign_members
				 from customer360.f_campaign_category
				 group by category_key, created_date_key)camp
			on a.category_key = camp.category_key
			and a.date_key = camp.created_date_key
		left join (select category_key, created_date_key+31536000 as created_date_key, sum(total_clp_credits) as clp_credits_earned, sum(campaign_leads + campaign_contacts) as campaign_members
				 from customer360.f_campaign_category
				 group by category_key, created_date_key+31536000)camp_ly
			on a.category_key = camp_ly.category_key
			and a.date_key = camp_ly.created_date_key
		left join (
				select 	case when gwcm_category like '%Facilities & Construction%' then 1
						when gwcm_category like '%Professional Services%' then 2
						when gwcm_category like '%IT%' then 3
						when gwcm_category like '%Medical%' then 4
						when gwcm_category like '%Transportation and Logistics Services%' then 5
						when gwcm_category like '%Industrial Products & Services%' then 6
						when gwcm_category like '%Travel%' then 7
						when gwcm_category like '%Security and Protection' then 8
						when gwcm_category like '%Human Capital%' then 9
						when gwcm_category like '%Office Management%' then 10
						else -1 end as category_key,
						date_part(epoch, date_signed::date) as date_key, 
						sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	case when gwcm_category like '%Facilities & Construction%' then 1
						when gwcm_category like '%Professional Services%' then 2
						when gwcm_category like '%IT%' then 3
						when gwcm_category like '%Medical%' then 4
						when gwcm_category like '%Transportation and Logistics Services%' then 5
						when gwcm_category like '%Industrial Products & Services%' then 6
						when gwcm_category like '%Travel%' then 7
						when gwcm_category like '%Security and Protection' then 8
						when gwcm_category like '%Human Capital%' then 9
						when gwcm_category like '%Office Management%' then 10
						else -1 end,
						date_part(epoch, date_signed::date) )fpds
			on a.category_key = fpds.category_key
				and a.date_key = fpds.date_key
		left join (
				select 	case when gwcm_category like '%Facilities & Construction%' then 1
						when gwcm_category like '%Professional Services%' then 2
						when gwcm_category like '%IT%' then 3
						when gwcm_category like '%Medical%' then 4
						when gwcm_category like '%Transportation and Logistics Services%' then 5
						when gwcm_category like '%Industrial Products & Services%' then 6
						when gwcm_category like '%Travel%' then 7
						when gwcm_category like '%Security and Protection' then 8
						when gwcm_category like '%Human Capital%' then 9
						when gwcm_category like '%Office Management%' then 10
						else -1 end as category_key, date_part(epoch, date_signed::date)+31536000 as date_key, sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	case when gwcm_category like '%Facilities & Construction%' then 1
						when gwcm_category like '%Professional Services%' then 2
						when gwcm_category like '%IT%' then 3
						when gwcm_category like '%Medical%' then 4
						when gwcm_category like '%Transportation and Logistics Services%' then 5
						when gwcm_category like '%Industrial Products & Services%' then 6
						when gwcm_category like '%Travel%' then 7
						when gwcm_category like '%Security and Protection' then 8
						when gwcm_category like '%Human Capital%' then 9
						when gwcm_category like '%Office Management%' then 10
						else -1 end, date_part(epoch, date_signed::date)+31536000 )fpds_ly
			on a.category_key = fpds_ly.category_key
				and a.date_key = fpds_ly.date_key
    group by a.category_key,
    		a.category_description,
		a.full_date::date; commit;""")
rows = conn.execute("select count(*) ct from customer360.v_category").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

