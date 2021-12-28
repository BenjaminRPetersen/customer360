# -*- coding: utf-8 -*-
"""
Title: Region Subject Area Redshift

Created on Wed Dec 15

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
cur.execute("""create view customer360.v_region as 
            select 	a.region::varchar,
		a.region_name,
		a.tax_returns_filed::varchar,
		a.total_wages::varchar,
		a.estimated_population::varchar,
		a.full_date::date,
		coalesce(sum(adv.extended_price),0)::bigint as total_advantage_spend,
		coalesce(sum(adv_ly.extended_price),0)::bigint as total_advantage_spend_ly,
		coalesce(sum(e.ebuy_rfqs_opened),0)::bigint as rfqs_opened,
		coalesce(sum(e_ly.ebuy_rfqs_opened),0)::bigint as rfqs_opened_ly,
		coalesce(sum(cs.ncsc_cases),0)::bigint as ncsc_cases_created,
		coalesce(sum(cs_ly.ncsc_cases),0)::bigint as ncsc_cases_created_ly,
		coalesce(sum(opp.opportunities),0)::bigint as opportunities,
		coalesce(sum(opp.opportunity_amount),0)::bigint as opportunity_size,
		coalesce(sum(opp_ly.opportunities),0)::bigint as opportunities_ly,
		coalesce(sum(opp_ly.opportunity_amount),0)::bigint as opportunity_size_ly,
		coalesce(sum(camp.campaign_members),0)::bigint as campaign_members,
		coalesce(sum(camp.clp_credits_earned),0)::bigint as clp_credits_earned,
		coalesce(sum(camp_ly.campaign_members),0)::bigint as campaign_members_ly,
		coalesce(sum(camp_ly.clp_credits_earned),0)::bigint as clp_credits_earned_ly,
		coalesce(sum(wv.webvisits),0)::bigint as web_sessions,
		coalesce(sum(wv.pages_visited),0)::bigint as web_pages_visited,
		coalesce(sum(wv_ly.webvisits),0)::bigint as web_sessions_ly,
		coalesce(sum(wv_ly.pages_visited),0)::bigint as web_pages_visited_ly,
		coalesce(sum(es.emailsends),0)::bigint as eloqua_emailsends,
		coalesce(sum(es_ly.emailsends),0)::bigint as eloqua_emailsends_ly,
		coalesce(sum(eo.emailopens),0)::bigint as eloqua_emailopens,
		coalesce(sum(eo_ly.emailopens),0)::bigint as eloqua_emailopens_ly,
		coalesce(sum(ec.emailclicks),0)::bigint as eloqua_emailclicks,
		coalesce(sum(ec_ly.emailclicks),0)::bigint as eloqua_emailclicks_ly,
		coalesce(sum(fpds.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated,
		coalesce(sum(fpds_ly.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated_ly
from 	(customer360.d_region
		cross join (select * from customer360.d_date where full_date between current_date-365 and current_date)) a --All Customers and Dates
		left join	--ADVANTAGE METRICS
			( 	select region, purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by region, purchase_date_key) adv
			on adv.region = a.region
			and a.date_key = adv.purchase_date_key
		left join	--ADVANTAGE METRICS
			( 	select region, purchase_date_key+31536000 as purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by region, purchase_date_key+31536000) adv_ly
			on adv_ly.region = a.region
			and a.date_key = adv_ly.purchase_date_key
		left join (	select region, issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by region, issue_day_key) e
			on a.region = e.region
			and a.date_key = e.issue_day_key
		left join (	select region, issue_day_key+31536000 as issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by region, issue_day_key+31536000) e_ly
			on a.region = e_ly.region
			and a.date_key = e_ly.issue_day_key
		left join (	select region,created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by region, created_date_key) cs
			on a.region = cs.region
			and a.date_key = cs.created_date_key
		left join (	select region,created_date_key+31536000 as created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by region, created_date_key+31536000) cs_ly
			on a.region = cs_ly.region
			and a.date_key = cs_ly.created_date_key
		left join (
					select region,created_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by region,created_date_key) opp
			on a.region = opp.region
			and a.date_key = opp.created_date_key
		left join (
					select region,created_date_key+31536000 as created_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by region,created_date_key+31536000) opp_ly
			on a.region = opp_ly.region
			and a.date_key = opp_ly.created_date_key
		left join (select region, created_date_key, sum(clp_credits_earned) as clp_credits_earned, count(distinct campaignmember_key) as campaign_members
				 from customer360.f_campaign
				 group by region, created_date_key)camp
			on a.region = camp.region
			and a.date_key = camp.created_date_key
		left join (select region, created_date_key+31536000 as created_date_key, sum(clp_credits_earned) as clp_credits_earned, count(distinct campaignmember_key) as campaign_members
				 from customer360.f_campaign
				 group by region, created_date_key+31536000)camp_ly
			on a.region = camp_ly.region
			and a.date_key = camp_ly.created_date_key
		left join (
				select 	region, activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	region, activity_date_key )wv
			on a.region = wv.region
				and a.date_key = wv.activity_date_key
		left join (
				select 	region, activity_date_key+31536000 as activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	region, activity_date_key+31536000 )wv_ly
			on a.region = wv_ly.region
				and a.date_key = wv_ly.activity_date_key
		left join (
				select 	region, activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	region, activity_date_key )es
			on a.region = es.region
				and a.date_key = es.activity_date_key
		left join (
				select 	region, activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	region, activity_date_key )eo
			on a.region = eo.region
				and a.date_key = eo.activity_date_key
		left join (
				select 	region, activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	region, activity_date_key )ec
			on a.region = ec.region
				and a.date_key = ec.activity_date_key
		left join (
				select 	region, activity_date_key+31536000 as activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	region, activity_date_key+31536000 )es_ly
			on a.region = es_ly.region
				and a.date_key = es_ly.activity_date_key
		left join (
				select 	region, activity_date_key+31536000 as activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	region, activity_date_key+31536000 )eo_ly
			on a.region = eo_ly.region
				and a.date_key = eo_ly.activity_date_key
		left join (
				select 	region, activity_date_key+31536000 as activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	region, activity_date_key+31536000 )ec_ly
			on a.region = ec_ly.region
				and a.date_key = ec_ly.activity_date_key
		left join (
				select 	co_region_id, date_part(epoch, date_signed::date) as date_key, sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	co_region_id, date_part(epoch, date_signed::date) )fpds
			on a.region = fpds.co_region_id
				and a.date_key = fpds.date_key
		left join (
				select 	co_region_id, date_part(epoch, date_signed::date)+31536000 as date_key, sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	co_region_id, date_part(epoch, date_signed::date)+31536000 )fpds_ly
			on a.region = fpds_ly.co_region_id
				and a.date_key = fpds_ly.date_key
    group by a.region::varchar,
		a.region_name,
		a.tax_returns_filed::varchar,
		a.total_wages::varchar,
		a.estimated_population::varchar,
		a.full_date::date; commit;""")
rows = conn.execute("select count(*) ct from customer360.v_region").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

