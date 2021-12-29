# -*- coding: utf-8 -*-
"""
Title: Customer Subject Area Redshift

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
cur.execute("""create or replace view customer360.v_case as
select 	core.full_date
		, survey.satisfaction_score as satisfaction_score
		, survey_ly.satisfaction_score as satisfaction_score_ly
		, rec.recommendation_score
		, rec_ly.recommendation_score as recommendation_score_ly
		, coalesce(sum(adv.extended_price),0)::bigint as total_advantage_spend
		, coalesce(sum(adv_ly.extended_price),0)::bigint as total_advantage_spend_ly
		, coalesce(sum(e.ebuy_rfqs_opened),0)::bigint as rfqs_issued
		, coalesce(sum(e_ly.ebuy_rfqs_opened),0)::bigint as rfqs_issued_ly
		, coalesce(sum(cs.ncsc_cases),0)::bigint as ncsc_cases_created
		, coalesce(sum(cs_ly.ncsc_cases),0)::bigint as ncsc_cases_created_ly
		, coalesce(sum(opp.opportunities),0)::bigint as closed_opportunities
		, coalesce(sum(opp.opportunity_amount),0)::bigint as closed_opportunity_size
		, coalesce(sum(opp_ly.opportunities),0)::bigint as closed_opportunities_ly
		, coalesce(sum(opp_ly.opportunity_amount),0)::bigint as closed_opportunity_size_ly
		, coalesce(sum(camp.campaign_members),0)::bigint as campaign_members
		, coalesce(sum(camp.clp_credits_earned),0)::bigint as clp_credits_earned
		, coalesce(sum(camp_ly.campaign_members),0)::bigint as campaign_members_ly
		, coalesce(sum(camp_ly.clp_credits_earned),0)::bigint as clp_credits_earned_ly
		, coalesce(sum(wv.webvisits),0)::bigint as web_sessions
		, coalesce(sum(wv.pages_visited),0)::bigint as web_pages_visited
		, coalesce(sum(wv_ly.webvisits),0)::bigint as web_sessions_ly
		, coalesce(sum(wv_ly.pages_visited),0)::bigint as web_pages_visited_ly
		, coalesce(sum(es.emailsends),0)::bigint as eloqua_emailsends
		, coalesce(sum(es_ly.emailsends),0)::bigint as eloqua_emailsends_ly
		, coalesce(sum(eo.emailopens),0)::bigint as eloqua_emailopens
		, coalesce(sum(eo_ly.emailopens),0)::bigint as eloqua_emailopens_ly
		, coalesce(sum(ec.emailclicks),0)::bigint as eloqua_emailclicks
		, coalesce(sum(ec_ly.emailclicks),0)::bigint as eloqua_emailclicks_ly
		, coalesce(sum(fpds.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated
		, coalesce(sum(fpds_ly.addressable_dollars_obligated),0)::bigint as addressable_dollars_obligated_ly
from		(select * from customer360.d_date where full_date between current_date-365 and current_date) core
		left join	--ADVANTAGE METRICS
			( 	select purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by purchase_date_key) adv
			on core.date_key = adv.purchase_date_key
		left join	--ADVANTAGE METRICS
			( 	select purchase_date_key+31536000 as purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by purchase_date_key+31536000) adv_ly
			on core.date_key = adv_ly.purchase_date_key
		left join (	select issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by issue_day_key) e
			on core.date_key = e.issue_day_key
		left join (	select issue_day_key+31536000 as issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by issue_day_key+31536000) e_ly
			on core.date_key = e_ly.issue_day_key
		left join (	select created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by created_date_key) cs
			on core.date_key = cs.created_date_key
		left join (	select created_date_key+31536000 as created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by created_date_key+31536000) cs_ly
			on core.date_key = cs_ly.created_date_key
		left join (
					select closed_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by closed_date_key) opp
			on core.date_key = opp.closed_date_key
		left join (
					select closed_date_key+31536000 as closed_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by closed_date_key+31536000) opp_ly
			on core.date_key = opp_ly.closed_date_key
		left join (select created_date_key, sum(total_clp_credits) as clp_credits_earned, sum(campaign_leads + campaign_contacts) as campaign_members
				 from customer360.f_campaign
				 group by created_date_key)camp
			on core.date_key = camp.created_date_key
		left join (select created_date_key+31536000 as created_date_key, sum(total_clp_credits) as clp_credits_earned, sum(campaign_leads + campaign_contacts) as campaign_members
				 from customer360.f_campaign
				 group by created_date_key+31536000)camp_ly
			on core.date_key = camp_ly.created_date_key
		left join (
				select 	activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	activity_date_key )wv
			on core.date_key = wv.activity_date_key
		left join (
				select 	activity_date_key+31536000 as activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	activity_date_key+31536000 )wv_ly
			on core.date_key = wv_ly.activity_date_key
		left join (
				select 	activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	activity_date_key )es
			on core.date_key = es.activity_date_key
		left join (
				select 	activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	activity_date_key )eo
			on core.date_key = eo.activity_date_key
		left join (
				select 	activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	activity_date_key )ec
			on core.date_key = ec.activity_date_key
		left join (
				select 	activity_date_key+31536000 as activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	activity_date_key+31536000 )es_ly
			on core.date_key = es_ly.activity_date_key
		left join (
				select 	activity_date_key+31536000 as activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	activity_date_key+31536000 )eo_ly
			on core.date_key = eo_ly.activity_date_key
		left join (
				select 	activity_date_key+31536000 as activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	activity_date_key+31536000 )ec_ly
			on core.date_key = ec_ly.activity_date_key
		cross join (	select 	avg(survey_answer) as satisfaction_score
					from 	customer360.f_survey
					where	question_key = 'Q27' and date_add('s',end_date_key,'1970-01-01') between current_date-365 and current_date) survey
		cross join (	select 	avg(survey_answer) as satisfaction_score
					from 	customer360.f_survey
					where	question_key = 'Q27' and date_add('s',end_date_key,'1970-01-01') between current_date-729 and current_date-366) survey_ly
		cross join (	select 	avg(survey_answer) as recommendation_score
					from 	customer360.f_survey
					where	question_key = 'Q25_Recommend' and date_add('s',end_date_key,'1970-01-01') between current_date-365 and current_date) rec
		cross join (	select 	avg(survey_answer) as recommendation_score
					from 	customer360.f_survey
					where	question_key = 'Q25_Recommend' and date_add('s',end_date_key,'1970-01-01') between current_date-729 and current_date-366) rec_ly
        left join (
				select 	date_part(epoch, date_signed::date) as date_key, sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	date_part(epoch, date_signed::date) )fpds
			on core.date_key = fpds.date_key
		left join (
				select 	date_part(epoch, date_signed::date)+31536000 as date_key, sum(dollars_obligated) as addressable_dollars_obligated
				from		customer360.s_fpds_obligations
				where	is_addressable = 'T'
				group by 	date_part(epoch, date_signed::date)+31536000 )fpds_ly
			on core.date_key = fpds_ly.date_key
group by core.full_date
		, survey.satisfaction_score
		, survey_ly.satisfaction_score
		, rec.recommendation_score
		, rec_ly.recommendation_score; commit;""")
rows = conn.execute("select count(*) ct from customer360.v_case;").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

