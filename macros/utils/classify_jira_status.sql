
{% macro classify_jira_status(status) -%}

CASE 
WHEN {{ status }} ="Résolu" THEN "Closed"
WHEN {{ status }} ="En cours" THEN "Open"
WHEN {{ status }} ="Care Team UATs" THEN "Open"
WHEN {{ status }} ="Rouvert" THEN "Open"
WHEN {{ status }} ="Need information" THEN "Open"
WHEN {{ status }} ="In our bugs backlog" THEN "Open"
WHEN {{ status }} ="Canceled" THEN "Canceled"
WHEN {{ status }} ="Annulé" THEN "Canceled"
WHEN {{ status }} ="Completed" THEN "Closed"
WHEN {{ status }} ="Pending" THEN "Open"
WHEN {{ status }} ="fix in progress" THEN "Open"
WHEN {{ status }} ="Escalated" THEN "Open"
WHEN {{ status }} ="Fermée" THEN "Closed"
WHEN {{ status }} ="Terminé" THEN "Closed"
WHEN {{ status }} ="Waiting for 1st answer" THEN "Open"
WHEN {{ status }} ="Terminé(e)" THEN "Closed"
WHEN {{ status }} ="Ouvert" THEN "Open"
WHEN {{ status }} = "FIX TO BE DEPLOYED" THEN "Open"
WHEN {{ status }} = "Resolved" THEN "Closed"
WHEN {{ status }} = "In Progress" THEN "Open"
WHEN {{ status }} = "Cancelled" THEN "Closed"
WHEN {{ status }} = "Reopened" THEN "Open"
WHEN {{ status }} = "Done" THEN "Closed"
ELSE {{ status }}
END

{% endmacro %}
