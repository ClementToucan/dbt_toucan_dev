{{config(materialized='table')}}


SELECT

    'daily' AS period,

    CAST(ymd AS STRING) AS date,

    interactions,

    new_users,

    current_users,

    reactivated_users,

    resurrected_users,

    at_risk_weekly_active_users,

    at_risk_monthly_active_users,

    dormant_users,

    active_users


FROM {{ref('user_activity_segments_interaction_daily')}}

UNION ALL

SELECT

    'weekly' AS period,

    y || '-W' || week_number AS date,

    interactions,

    new_users,

    current_users,

    reactivated_users,

    resurrected_users,

    at_risk_weekly_active_users,

    at_risk_monthly_active_users,

    dormant_users,

    active_users


FROM {{ref('user_activity_segments_interaction_weekly')}}

UNION ALL

SELECT

    'monthly' AS period,

    y || '-' || month_number AS date,

    interactions,
    
    new_users,

    current_users,

    reactivated_users,

    resurrected_users,

    at_risk_weekly_active_users,

    at_risk_monthly_active_users,

    dormant_users,

    active_users


FROM {{ref('user_activity_segments_interaction_monthly')}} 
