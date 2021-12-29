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
cur.execute("""create or replace view customer360.v_customer as
select 	agency.agency_name
        , core.customer_key
		, core.zip_code
		, core.region
		, core.region_name
		, core.email_domain
		, core.full_date
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
from		(select u.*, d.full_date, c.zip_code, c.region, c.region_name, c.email_domain, c.agency_key  from ( 
		  select distinct customer_key, purchase_date_key as date_key
		  from customer360.f_advantage
		  union
		  select distinct customer_key, purchase_date_key+31536000 as date_key
		  from customer360.f_advantage
		  union
		  select 	distinct customer_key, issue_day_key as date_key
		  from	customer360.f_ebuy
		  union
		  select distinct customer_key, issue_day_key+31536000 as date_key
		  from customer360.f_ebuy
		  union
		  select distinct customer_key,created_date_key as date_key 
		  from customer360.f_case
		  union
		  select distinct customer_key,created_date_key+31536000 as date_key 
		  from customer360.f_case
		  union
		  select distinct customer_key,closed_date_key as date_key 
		  from customer360.f_opportunity
		  union
		  select distinct customer_key,closed_date_key+31536000 as date_key 
		  from customer360.f_opportunity
		  union
		  select distinct customer_key, created_date_key as date_key
		  from customer360.f_campaign_member
		  union
		  select distinct customer_key, created_date_key+31536000 as date_key
		  from customer360.f_campaign_member
		  union
		  select 	distinct customer_key, activity_date_key as date_key
		  from		customer360.f_eloqua_webvisit
		  union
		  select 	distinct customer_key, activity_date_key+31536000 as date_key
		  from		customer360.f_eloqua_webvisit
		  union
		  select 	distinct customer_key, activity_date_key as date_key
		  from		customer360.f_eloqua_emailsend
		  union
		  select 	distinct customer_key, activity_date_key+31536000 as date_key
		  from		customer360.f_eloqua_emailsend
		  union
		  select 	distinct customer_key, activity_date_key as date_key
		  from		customer360.f_eloqua_emailopen
		  union
		  select 	distinct customer_key, activity_date_key+31536000 as date_key
		  from		customer360.f_eloqua_emailopen
		  union
		  select 	distinct customer_key, activity_date_key as date_key
		  from		customer360.f_eloqua_emailclick
		  union
		  select 	distinct customer_key, activity_date_key+31536000 as date_key
		  from		customer360.f_eloqua_emailclick
		  union
		  select 	distinct customer_key, end_date_key as date_key
		  from 		customer360.f_survey
		  where	question_key in ('Q27', 'Q25_Recommend')) u
		join (select * from customer360.d_date where full_date between current_date-365 and current_date) d
			on u.date_key = d.date_key
        left join customer360.d_customer c
            on c.customer_key = u.customer_key ) core
		left join 
			customer360.d_agency agency
			on agency.agency_key = core.agency_key
		left join	--ADVANTAGE METRICS
			( 	select customer_key, purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by customer_key, purchase_date_key) adv
			on adv.customer_key = core.customer_key
			and core.date_key = adv.purchase_date_key
		left join	--ADVANTAGE METRICS
			( 	select customer_key, purchase_date_key+31536000 as purchase_date_key, sum(extended_price) as extended_price
				from customer360.f_advantage
				group by customer_key, purchase_date_key+31536000) adv_ly
			on adv_ly.customer_key = core.customer_key
			and core.date_key = adv_ly.purchase_date_key
		left join (	select customer_key, issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by customer_key, issue_day_key) e
			on core.customer_key = e.customer_key
			and core.date_key = e.issue_day_key
		left join (	select customer_key, issue_day_key+31536000 as issue_day_key, count(distinct rfq_key) as ebuy_rfqs_opened
					from	customer360.f_ebuy
					group by customer_key, issue_day_key+31536000) e_ly
			on core.customer_key = e_ly.customer_key
			and core.date_key = e_ly.issue_day_key
		left join (	select customer_key,created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by customer_key, created_date_key) cs
			on core.customer_key = cs.customer_key
			and core.date_key = cs.created_date_key
		left join (	select customer_key,created_date_key+31536000 as created_date_key, count(distinct case_key) as ncsc_cases 
					from customer360.f_case 
					group by customer_key, created_date_key+31536000) cs_ly
			on core.customer_key = cs_ly.customer_key
			and core.date_key = cs_ly.created_date_key
		left join (
					select customer_key,closed_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by customer_key,closed_date_key) opp
			on core.customer_key = opp.customer_key
			and core.date_key = opp.closed_date_key
		left join (
					select customer_key,closed_date_key+31536000 as closed_date_key, count(distinct opportunity_key)::bigint as opportunities, sum(amount) as opportunity_amount 
					from customer360.f_opportunity 
					group by customer_key,closed_date_key+31536000) opp_ly
			on core.customer_key = opp_ly.customer_key
			and core.date_key = opp_ly.closed_date_key
		left join (select customer_key, created_date_key, sum(clp_credits_earned) as clp_credits_earned, count(distinct campaignmember_key) as campaign_members
				 from customer360.f_campaign_member
				 group by customer_key, created_date_key)camp
			on core.customer_key = camp.customer_key
			and core.date_key = camp.created_date_key
		left join (select customer_key, created_date_key+31536000 as created_date_key, count(distinct campaign_key) as campaigns, sum(clp_credits_earned) as clp_credits_earned, count(distinct campaignmember_key) as campaign_members
				 from customer360.f_campaign_member
				 group by customer_key, created_date_key+31536000)camp_ly
			on core.customer_key = camp_ly.customer_key
			and core.date_key = camp_ly.created_date_key
		left join (
				select 	customer_key, activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	customer_key, activity_date_key )wv
			on core.customer_key = wv.customer_key
				and core.date_key = wv.activity_date_key
		left join (
				select 	customer_key, activity_date_key+31536000 as activity_date_key, count(distinct webvisit_key) as webvisits, sum(numberofpages) as pages_visited
				from		customer360.f_eloqua_webvisit
				group by 	customer_key, activity_date_key+31536000 )wv_ly
			on core.customer_key = wv_ly.customer_key
				and core.date_key = wv_ly.activity_date_key
		left join (
				select 	customer_key, activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	customer_key, activity_date_key )es
			on core.customer_key = es.customer_key
				and core.date_key = es.activity_date_key
		left join (
				select 	customer_key, activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	customer_key, activity_date_key )eo
			on core.customer_key = eo.customer_key
				and core.date_key = eo.activity_date_key
		left join (
				select 	customer_key, activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	customer_key, activity_date_key )ec
			on core.customer_key = ec.customer_key
				and core.date_key = ec.activity_date_key
		left join (
				select 	customer_key, activity_date_key+31536000 as activity_date_key, count(distinct emailsend_key) as emailsends
				from		customer360.f_eloqua_emailsend
				group by 	customer_key, activity_date_key+31536000 )es_ly
			on core.customer_key = es_ly.customer_key
				and core.date_key = es_ly.activity_date_key
		left join (
				select 	customer_key, activity_date_key+31536000 as activity_date_key, count(distinct emailopen_key) as emailopens
				from		customer360.f_eloqua_emailopen
				group by 	customer_key, activity_date_key+31536000 )eo_ly
			on core.customer_key = eo_ly.customer_key
				and core.date_key = eo_ly.activity_date_key
		left join (
				select 	customer_key, activity_date_key+31536000 as activity_date_key, count(distinct emailclick_key) as emailclicks
				from		customer360.f_eloqua_emailclick
				group by 	customer_key, activity_date_key+31536000 )ec_ly
			on core.customer_key = ec_ly.customer_key
				and core.date_key = ec_ly.activity_date_key
		left join (	select 	customer_key, avg(survey_answer) as satisfaction_score
					from 	customer360.f_survey
					where	question_key = 'Q27' and date_add('s',end_date_key,'1970-01-01') between current_date-365 and current_date
					group by 	customer_key) survey
			on core.customer_key = survey.customer_key
		left join (	select 	customer_key, avg(survey_answer) as satisfaction_score
					from 	customer360.f_survey
					where	question_key = 'Q27' and date_add('s',end_date_key,'1970-01-01') between current_date-729 and current_date-366
					group by 	customer_key) survey_ly
			on core.customer_key = survey_ly.customer_key 
		left join (	select 	customer_key, avg(survey_answer) as recommendation_score
					from 	customer360.f_survey
					where	question_key = 'Q25_Recommend' and date_add('s',end_date_key,'1970-01-01') between current_date-365 and current_date
					group by 	customer_key) rec
			on core.customer_key = rec.customer_key
		left join (	select 	customer_key, avg(survey_answer) as recommendation_score
					from 	customer360.f_survey
					where	question_key = 'Q25_Recommend' and date_add('s',end_date_key,'1970-01-01') between current_date-729 and current_date-366
					group by 	customer_key) rec_ly
			on core.customer_key = rec_ly.customer_key 
group by agency.agency_name
        , core.customer_key
		, core.zip_code
		, core.region
		, core.region_name
		, core.email_domain
		, core.full_date
		, survey.satisfaction_score
		, survey_ly.satisfaction_score
		, rec.recommendation_score
		, rec_ly.recommendation_score; commit;""")
rows = conn.execute("select count(*) ct from customer360.v_customer;").fetchall()[0][0]
end = datetime.datetime.now()
difference = (end-start)
difference = difference.total_seconds()

