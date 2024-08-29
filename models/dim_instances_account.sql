{{ config(materialized='table') }}



WITH first_dim_instances_table AS (

SELECT

    instance_usage,

    toucan_version,

    instance,

    client,

    client_country,

    instance_status,

    analytics_flag,

    embed_flag,

    CASE
    WHEN frontend_vhost IS NULL THEN instance
    ELSE frontend_vhost
    END AS frontend_vhost,

    client_region





FROM {{ref('dim_instances')}}

WHERE instance NOT IN ('robomat-preprod', 'willaman.', 'lizee', 'rugby', 'procivis-toucantoco-com.translate.goog', 'github', 'cgi.', 'aws-demo','dashboard-lengo-ai.translate.goog',
'sandbox-test-move-duplicate', 'snowflake', 'alcopa--auction-toucantoco-com.translate.goog', 'github', 'nanoblue', 'systra','civical', 'codata', 'prerequis','nanoblue', 'systra','civical', 'codata')

-- AND frontend_vhost NOT IN ('cegid-optim.toucantoco.guru')

AND client != 'ngrok'

),

second_dim_instances_table AS (

SELECT

  instance_usage,

    toucan_version,

    instance,

    client,

    client_country,

    instance_status,

    analytics_flag,

    embed_flag,

    client_region,

    CASE
    WHEN frontend_vhost = 'datavision-economie-gouv-fr.translate.goog' THEN 'datavision.economie.gouv.fr'
    WHEN frontend_vhost = 'france-travail' THEN 'francetravail.toucantoco.com'
    WHEN frontend_vhost = 'nexity-sp' THEN 'nexity-sp.toucantoco.com'
    WHEN frontend_vhost = 'prefecture-idf' THEN 'prefecture-idf.toucantoco.com'
    WHEN frontend_vhost = 'totalenergies' THEN 'totalenergies.toucantoco.com'
    WHEN frontend_vhost = 'ditp' THEN 'ditp.toucantoco.com'
    WHEN frontend_vhost = 'sodexo' THEN 'sodexo.toucantoco.com'
    WHEN frontend_vhost = 'verifiedmarketresearch' THEN 'verifiedmarketresearch.toucantoco.com'
    WHEN frontend_vhost = 'cats-loire-haute-loire.' THEN 'cats-loire-haute-loire.toucantoco.com'
    WHEN frontend_vhost = 'lifeconnectionofohio' THEN 'lifeconnectionofohio.toucantoco.com'
    WHEN frontend_vhost = 'newseason' THEN 'newseason.toucantoco.com'
    WHEN frontend_vhost = 'famoco' THEN 'famoco.toucantoco.com'
    WHEN frontend_vhost = 'intodata' THEN 'intodata.toucantoco.com'
    WHEN frontend_vhost = 'momentslab' THEN 'momentslab.toucantoco.com'
    WHEN frontend_vhost = 'issforphilips' THEN 'issforphilips.toucantoco.com'
    WHEN frontend_vhost = 'visionweb' THEN 'visionweb.toucantoco.com'
    WHEN frontend_vhost = 'truelytics' THEN 'truelytics.toucantoco.com'
    WHEN frontend_vhost = 'wynd' THEN 'wynd.toucantoco.com'
    WHEN frontend_vhost = 'nexecur' THEN 'nexecur.toucantoco.com'
    WHEN frontend_vhost = 'tradematch' THEN 'tradematch.toucantoco.com'
    WHEN frontend_vhost = 'provicis' THEN 'procivis.toucantoco.com'
    WHEN frontend_vhost = 'mobilizepowersolutions' THEN 'mobilizepowersolutions.toucantoco.com'
    WHEN frontend_vhost = 'monnie-ennergies' THEN 'monnier-energies.toucantoco.com'
    WHEN frontend_vhost = 'ventacity' THEN 'ventacity.toucantoco.com'
    WHEN frontend_vhost = 'wave.webaim.org' THEN 'netwave.toucantoco.com'
    WHEN frontend_vhost = 'stellantis-owned-retail' THEN 'stellantis-owned-retail.toucantoco.com'
    WHEN frontend_vhost = '360suite' THEN '360suite.toucantoco.com'
    WHEN frontend_vhost = 'clubspark' THEN 'clubspark.toucantoco.com'
    WHEN frontend_vhost = 'interlace-health' THEN 'interlace-health.toucantoco.com'
    WHEN frontend_vhost = 'knave' THEN 'knave.toucantoco.com'
    WHEN frontend_vhost = 'data-territoire-public' THEN 'data-territoire-public.toucantoco.com'
    WHEN frontend_vhost = 'francetravail' THEN 'francetravail.toucantoco.com'
    WHEN frontend_vhost = 'sncf-telecom' THEN 'sncf-telecom.toucantoco.com'
    WHEN frontend_vhost = 'nouvellesdonnes' THEN 'nouvellesdonnes.toucantoco.com'
    WHEN frontend_vhost = 'optiisolutions' THEN 'optiisolutions.toucantoco.com'
    WHEN frontend_vhost = 'eteck' THEN 'eteck.toucantoco.com'
    WHEN frontend_vhost = 'strada' THEN 'strada.toucantoco.com'
    WHEN frontend_vhost = 'te46-public' THEN 'te46.toucantoco.com'
    WHEN frontend_vhost = 'verisinsights' THEN 'verisinsights.toucantoco.com'
    WHEN frontend_vhost = 'purse-v3' THEN 'purse-v3.toucantoco.com'
    WHEN frontend_vhost = 'astrazeneca' THEN 'astrazeneca.toucantoco.com'
    WHEN frontend_vhost = 'stratumn' THEN 'stratumn.toucantoco.com'
    WHEN frontend_vhost = 'cleyrop' THEN 'cleyrop.toucantoco.com'
    WHEN frontend_vhost = 'granbury' THEN 'granbury.toucantoco.com'
    WHEN frontend_vhost = 'orange' THEN 'orange.toucantoco.com'
    WHEN frontend_vhost = 'sncf-innovation' THEN 'sncf-innovation.toucantoco.com'
    WHEN frontend_vhost = 'analytics.verifiedmarketresearch.com' THEN 'verifiedmarketresearch.toucantoco.com'
    WHEN frontend_vhost = 'cgi' THEN 'cgi.toucantoco.com'
    WHEN frontend_vhost = 'fiminco' THEN 'fiminco.toucantoco.com'
    WHEN frontend_vhost = 'euler-hermes' THEN 'euler-hermes.toucantoco.com'
    WHEN frontend_vhost = 'mydata' THEN 'mydata.toucantoco.com'
    WHEN frontend_vhost = 'tnevision' THEN 'tnevision.toucantoco.com'
    WHEN frontend_vhost = 'coty' THEN 'coty.toucantoco.com'
    WHEN frontend_vhost = 'lacoste' THEN 'lacoste.toucantoco.com'
    WHEN frontend_vhost = 'rok-solutions' THEN 'rok-solutions.toucantoco.com'
    WHEN frontend_vhost = 'sncf' THEN 'sncf.toucantoco.com'
    WHEN frontend_vhost = 'stratumn-trace' THEN 'stratumn-trace.toucantoco.com'
    WHEN frontend_vhost = 'ogf' THEN 'ogf.toucantoco.com'
    WHEN frontend_vhost = 'tamarind' THEN 'tamarind.toucantoco.com'
    WHEN frontend_vhost = 'rpa-h4h' THEN 'rpa-h4h.toucantoco.com'
    WHEN frontend_vhost = 'hdb' THEN 'hdb.toucantoco.com'
    WHEN frontend_vhost = 'scc' THEN 'scc.toucantoco.com'
    WHEN frontend_vhost = 'engage.connectedcircles.net' THEN 'connectedcircles.toucantoco.com'
    WHEN frontend_vhost = 'hector-forvia' THEN 'hector-forvia.toucantoco.com'
    WHEN frontend_vhost = 'ipsen' THEN 'ipsen.toucantoco.com'
    WHEN frontend_vhost = 'cervecerialatropical' THEN 'cervecerialatropical.toucantoco.com'
    WHEN frontend_vhost = 'francedigitale' THEN 'francedigitale.toucantoco.com'
    WHEN frontend_vhost = 'decathlon' THEN 'decathlon.toucantoco.com'
    WHEN frontend_vhost = 'libeo' THEN 'libeo.toucantoco.com'
    WHEN frontend_vhost = 'vo2' THEN 'vo2.toucantoco.com'
    WHEN frontend_vhost = 'weborama' THEN 'weborama.toucantoco.com'
    WHEN frontend_vhost = 'cstbgroup' THEN 'cstbgroup.toucantoco.com'
    WHEN frontend_vhost = 'tf1' THEN 'tf1.toucantoco.com'
    WHEN frontend_vhost = 'interfas-display' THEN 'interfas.toucantoco.com'
    WHEN frontend_vhost = 'monnier-energies' THEN 'monnier-energies.toucantoco.com'
    WHEN frontend_vhost = 'showagroup' THEN 'showagroup.toucantoco.com'
    WHEN frontend_vhost = 'teamwill' THEN 'teamwill.toucantoco.com'
    WHEN frontend_vhost = 'bord-eau-village' THEN 'bord-eau-village.toucantoco.com'
    ELSE frontend_vhost
    END AS frontend_vhost


FROM first_dim_instances_table

),

instance_account_table AS (

SELECT

    account_name,

    account_id,

    CASE
    WHEN instance_url_domain = 'euler-hermes.toucantoco.com' THEN 'italiadelleimprese.eulerhermes.com'
    ELSE instance_url_domain
    END AS instance_url_domain


FROM `data-finance-staging.prod_tctc_salesforce.fact_account_with_instances_licences`



)

SELECT

    a.*,


    CASE
    WHEN frontend_vhost = 'douanes.toucantoco.com' THEN "Ministère De L'Economie Des Finances Et De La Relance"
    WHEN frontend_vhost = 'bilan.programme-cee-actee.fr' THEN 'Fnccr'
    WHEN frontend_vhost = 'h4h' THEN 'Humans4help (H4h)'
    WHEN frontend_vhost = 'ditp.toucantoco.com' THEN 'Ditp - Direction Interministérielle De La Transformation Publique'

    WHEN frontend_vhost = 'sodexo.toucantoco.com' THEN 'Sodexo'
    WHEN frontend_vhost = 'verifiedmarketresearch.toucantoco.com' THEN 'Verified Market Research'
    WHEN frontend_vhost = 'cats-loire-haute-loire.toucantoco.com' THEN 'Crédit Agricole Loire Haute Loire'
    WHEN frontend_vhost = 'lifeconnectionofohio.toucantoco.com' THEN 'Life Connection of Ohio'
    WHEN frontend_vhost = 'famoco.toucantoco.com' THEN 'Famoco'
    WHEN frontend_vhost = 'intodata.toucantoco.com' THEN 'IntoData'
    WHEN frontend_vhost = 'momentslab.toucantoco.com' THEN 'Newsbridge'
    WHEN frontend_vhost = 'issforphilips.toucantoco.com' THEN 'Iss A/s'
    WHEN frontend_vhost = 'newseason.toucantoco.com' THEN 'New Season'
    WHEN frontend_vhost = 'visionweb.toucantoco.com' THEN 'Visionweb'
    WHEN frontend_vhost = 'apps.odasa.com.au' THEN 'Endeavour Energy (Nsw)'
    WHEN frontend_vhost = 'truelytics.toucantoco.com' THEN 'Truelytics'
    WHEN frontend_vhost = 'wynd.toucantoco.com' THEN 'Wynd'
    WHEN frontend_vhost = 'intoanalytics.toucantoco.com' THEN 'IntoData'
    WHEN frontend_vhost = 'nexecur.toucantoco.com' THEN 'Nexecur (Ca)'
    WHEN frontend_vhost = 'tradematch.toucantoco.com' THEN 'Euler Hermes'
    WHEN frontend_vhost IN ('tradematch.allianz-trade.com','tradematch.eulerhermes.com') THEN 'Euler Hermes'
    WHEN frontend_vhost = 'procivis.toucantoco.com' THEN 'Procivis'
    WHEN frontend_vhost = 'banque-des-territoires.toucantoco.com' THEN 'Caisse Des Dépôts'
    WHEN frontend_vhost = 'mobilizepowersolutions.toucantoco.com' THEN 'MOBILIZE Power Solutions - Ex-Elexent'
    WHEN frontend_vhost = 'monnier-energies.toucantoco.com' THEN 'Monnier Energies'
    WHEN frontend_vhost = 'ensae-dataviz.toucantoco.com' THEN 'Ensae'
    WHEN frontend_vhost = 'ventacity.toucantoco.com' THEN 'Ventacity Systems'
    WHEN frontend_vhost = 'netwave.toucantoco.com' THEN 'Netwave'
    WHEN frontend_vhost = 'stellantis-owned-retail.toucantoco.com' THEN 'Psa Retail'
    WHEN frontend_vhost = 'swp.toucantoco.com' THEN 'Psa Retail'
    WHEN frontend_vhost = 'eosspacelink.toucantoco.com' THEN 'EOS (Electro Optic Systems)'
    WHEN frontend_vhost = '360suite.toucantoco.com' THEN '360Suite'
    WHEN frontend_vhost = 'clubspark.toucantoco.com' THEN 'Clubspark'
    WHEN frontend_vhost = 'interlace-health.toucantoco.com' THEN 'Interlace Health'
    WHEN frontend_vhost = 'knave.toucantoco.com' THEN 'Knave'
    WHEN frontend_vhost IN ('www.data-territoire.fr','data-territoire-public.toucantoco.com') THEN 'Infopro Digital'
    WHEN frontend_vhost = 'francetravail.toucantoco.com' THEN 'Pôle Emploi'
    WHEN frontend_vhost = 'sncf-telecom.toucantoco.com' THEN 'Sncf Réseau'
    WHEN frontend_vhost IN ('mindyourreceivables.allianz-trade.com','mindyourreceivables.eulerhermes.com') THEN 'Euler Hermes'
    WHEN frontend_vhost = 'nouvellesdonnes.toucantoco.com' THEN 'Nouvelles Donnes'
    WHEN frontend_vhost = 'optiisolutions.toucantoco.com' THEN 'Optii Solutions'
    WHEN frontend_vhost = 'hometap.toucantoco.com' THEN 'Hometap'
    WHEN frontend_vhost = 'eteck.toucantoco.com' THEN 'Eteck'
    WHEN frontend_vhost = 'strada.toucantoco.com' THEN 'Strada'
    WHEN frontend_vhost = 'te46.toucantoco.com' THEN "FDEL - Territoire d'Énergie Lot"
    WHEN frontend_vhost = 'verisinsights.toucantoco.com' THEN 'Veris Insights (Ivy Research Council)'
    WHEN frontend_vhost = 'theventurelane.toucantoco.com' THEN 'Venture Lane'
    WHEN frontend_vhost = 'purse-v3.toucantoco.com' THEN 'Purse (prev : Upstreampay)'
    WHEN frontend_vhost = 'astrazeneca.toucantoco.com' THEN 'Astrazeneca'
    WHEN frontend_vhost = 'stratumn.toucantoco.com' THEN 'Stratumn'
    WHEN frontend_vhost = 'predictik.toucantoco.com' THEN 'Arion technologies'
    WHEN frontend_vhost = 'cleyrop.toucantoco.com' THEN 'Cleyrop / EX Datagemme'
    WHEN frontend_vhost = 'granbury.toucantoco.com' THEN 'Granbury Solutions'
    WHEN frontend_vhost = 'orange.toucantoco.com' THEN 'Orange Sa'
    WHEN frontend_vhost = 'sncf-innovation.toucantoco.com' THEN 'Sncf (Groupe)'
    WHEN frontend_vhost = 'loreal-digital.toucantoco.com' THEN "L'Oréal"
    WHEN frontend_vhost = 'euler-hermes.toucantoco.com' THEN 'Euler Hermes Italia'
    WHEN frontend_vhost = 'tnevision.toucantoco.com' THEN 'Tne Vision'
    WHEN frontend_vhost = 'rok-solutions.toucantoco.com' THEN 'Rok-Solution'
    WHEN frontend_vhost = 'alma.toucantoco.com' THEN 'Alma'
    WHEN frontend_vhost = 'tamarind.toucantoco.com' THEN 'Tamarind Intelligence'
    WHEN frontend_vhost = 'pinpoint.toucantoco.com' THEN 'PINpoint Information Systems'
    WHEN frontend_vhost = 'remmelzwaan.toucantoco.com' THEN 'Planalyse B.v.'
    WHEN frontend_vhost = 'rpa-h4h.toucantoco.com' THEN 'Humans4help (H4h)'
    WHEN frontend_vhost = 'axa-im.toucantoco.com' THEN 'Axa Im'
    WHEN frontend_vhost = 'publishing-profits.toucantoco.com' THEN 'Publishing Profits, LLC'
    WHEN frontend_vhost = 'quantmetry.toucantoco.com' THEN 'Capgemini'
    WHEN frontend_vhost = 'ey-italy.toucantoco.com' THEN 'Ey Italie'
    WHEN frontend_vhost = 'pricehubble.toucantoco.com	' THEN 'Pricehubble Ag'
    WHEN frontend_vhost = 'decathlon.toucantoco.com' THEN 'Decathlon'
    WHEN frontend_vhost = 'vo2.toucantoco.com' THEN 'Vo2 Group'
    WHEN frontend_vhost = 'mindsdb.toucantoco.com' THEN 'MindsDB'
    WHEN frontend_vhost = 'nexusdemo.toucantoco.com' THEN 'Nexus A.i'
    WHEN frontend_vhost = 'cstbgroup.toucantoco.com' THEN 'Cstb'
    WHEN frontend_vhost = 'idealis-consulting.toucantoco.com' THEN 'Idealis Consulting'
    WHEN frontend_vhost = 'interieur.toucantoco.com' THEN "Ministère De L'Intérieur"
    WHEN frontend_vhost = 'oxatis.toucantoco.com' THEN 'Oxatis'
    WHEN frontend_vhost = 'monnier-energies.toucantoco.com' THEN 'Monnier Energies'
    WHEN frontend_vhost = 'showagroup.toucantoco.com' THEN 'Showa Group'
    WHEN frontend_vhost = 'teamwill.toucantoco.com' THEN 'Teamwill Consulting'

    ELSE b.account_name
    END AS account_name,

    CASE
    WHEN frontend_vhost = 'douanes.toucantoco.com' THEN '00109000005fsegAAA'
    WHEN frontend_vhost = 'bilan.programme-cee-actee.fr' THEN '00109000005fz16AAA'
    WHEN frontend_vhost = 'h4h' THEN '00109000005g107AAA'
    WHEN frontend_vhost = 'ditp.toucantoco.com' THEN '00109000005ft1rAAA'
    WHEN frontend_vhost = 'sodexo.toucantoco.com' THEN '00109000005fu8YAAQ'
    WHEN frontend_vhost = 'verifiedmarketresearch.toucantoco.com' THEN '00109000005fznPAAQ'
    WHEN frontend_vhost = 'cats-loire-haute-loire.toucantoco.com' THEN '00109000005furmAAA'
    WHEN frontend_vhost = 'lifeconnectionofohio.toucantoco.com' THEN '0010900000BwgJRAAZ'
    WHEN frontend_vhost = 'famoco.toucantoco.com'  THEN '00109000005fsPJAAY'
    WHEN frontend_vhost = 'intodata.toucantoco.com' THEN '0010900000WIeRLAA1'
    WHEN frontend_vhost = 'momentslab.toucantoco.com' THEN '001IV00000L9QFVYA3'
    WHEN frontend_vhost = 'issforphilips.toucantoco.com' THEN '00109000005fxilAAA'
    WHEN frontend_vhost = 'newseason.toucantoco.com' THEN '0010900000EKhe5AAD'
    WHEN frontend_vhost = 'visionweb.toucantoco.com' THEN '00109000005fvu0AAA'
    WHEN frontend_vhost = 'apps.odasa.com.au' THEN '00109000005g1cmAAA'
    WHEN frontend_vhost = 'truelytics.toucantoco.com' THEN '00109000005fxoTAAQ'
    WHEN frontend_vhost = 'wynd.toucantoco.com' THEN '0010900000BwzolAAB'
    WHEN frontend_vhost = 'intoanalytics.toucantoco.com' THEN '0010900000WIeRLAA1'
    WHEN frontend_vhost = 'nexecur.toucantoco.com' THEN '00109000005futzAAA'
    WHEN frontend_vhost = 'tradematch.toucantoco.com' THEN '00109000005fsIIAAY'
    WHEN frontend_vhost IN ('tradematch.allianz-trade.com','tradematch.eulerhermes.com') THEN '00109000005fsIIAAY'
    WHEN frontend_vhost = 'ca-indosuez.toucantoco.com	' THEN '00109000005fsXbAAI'
    WHEN frontend_vhost = 'procivis.toucantoco.com' THEN '00109000005g1j8AAA'
    WHEN frontend_vhost = 'banque-des-territoires.toucantoco.com' THEN '00109000005fs3EAAQ'
    WHEN frontend_vhost = 'mobilizepowersolutions.toucantoco.com' THEN '00109000005fznBAAQ'
    WHEN frontend_vhost = 'monnier-energies.toucantoco.com' THEN '0010900001HFAOvAAP'
    WHEN frontend_vhost = 'ensae-dataviz.toucantoco.com' THEN '00109000005fsGYAAY'
    WHEN frontend_vhost = 'ventacity.toucantoco.com' THEN '0010900000Bxn2cAAB'
    WHEN frontend_vhost = 'netwave.toucantoco.com' THEN '0010900000WJWQGAA5'
    WHEN frontend_vhost = 'stellantis-owned-retail.toucantoco.com' THEN '00109000005fxHoAAI'
    WHEN frontend_vhost = 'swp.toucantoco.com' THEN '00109000005fxHoAAI'
    WHEN frontend_vhost = 'eosspacelink.toucantoco.com' THEN '001IV00000e579tYAA'
    WHEN frontend_vhost = '360suite.toucantoco.com' THEN '00109000005fyIwAAI'
    WHEN frontend_vhost = 'clubspark.toucantoco.com' THEN '00109000005fyOgAAI'
    WHEN frontend_vhost = 'interlace-health.toucantoco.com' THEN '0010900000pgEV3AAM'
    WHEN frontend_vhost = 'knave.toucantoco.com' THEN '0015q000008ypglAAA'
    WHEN frontend_vhost IN ('www.data-territoire.fr','data-territoire-public.toucantoco.com') THEN '00109000005frzjAAA'
    WHEN frontend_vhost = 'francetravail.toucantoco.com' THEN '00109000005ftAfAAI'
    WHEN frontend_vhost = 'sncf-telecom.toucantoco.com' THEN '00109000005fsjtAAA'
    WHEN frontend_vhost IN ('mindyourreceivables.allianz-trade.com', 'mindyourreceivables.eulerhermes.com')THEN '00109000005fsIIAAY'
    WHEN frontend_vhost = 'nouvellesdonnes.toucantoco.com' THEN '00109000005fwMiAAI'
    WHEN frontend_vhost = 'optiisolutions.toucantoco.com' THEN '00109000005fykDAAQ'
    WHEN frontend_vhost = 'hometap.toucantoco.com' THEN '0010900000vpcDkAAI'
    WHEN frontend_vhost = 'eteck.toucantoco.com' THEN '00109000005fvB2AAI'
    WHEN frontend_vhost = 'strada.toucantoco.com' THEN '00109000005fvMSAAY'
    WHEN frontend_vhost = 'te46.toucantoco.com' THEN '00109000009kuNjAAI'
    WHEN frontend_vhost = 'verisinsights.toucantoco.com' THEN '00109000005g2esAAA'
    WHEN frontend_vhost = 'theventurelane.toucantoco.com' THEN '00109000005fvBsAAI'
    WHEN frontend_vhost = 'purse-v3.toucantoco.com' THEN '00109000020PlwPAAS'
    WHEN frontend_vhost = 'astrazeneca.toucantoco.com' THEN '00109000005fvEbAAI'
    WHEN frontend_vhost = 'stratumn.toucantoco.com' THEN '00109000005fx6bAAA'
    WHEN frontend_vhost = 'predictik.toucantoco.com' THEN '0010900000GgGPiAAN'
    WHEN frontend_vhost = 'cleyrop.toucantoco.com' THEN '00109000005g1ZfAAI'
    WHEN frontend_vhost = 'granbury.toucantoco.com' THEN '0010900000WK6RnAAL'
    WHEN frontend_vhost = 'orange.toucantoco.com' THEN '00109000005fspvAAA'
    WHEN frontend_vhost = 'sncf-innovation.toucantoco.com' THEN '00109000005ftqcAAA'
    WHEN frontend_vhost = 'loreal-digital.toucantoco.com' THEN '00109000005fvDMAAY'
    WHEN frontend_vhost = 'euler-hermes.toucantoco.com' THEN '00109000005ftCWAAY'
    WHEN frontend_vhost = 'tnevision' THEN '00109000005fv6aAAA'
    WHEN frontend_vhost = 'rok-solutions.toucantoco.com' THEN '00109000005ftwYAAQ'
    WHEN frontend_vhost = 'alma.toucantoco.com' THEN '00109000005fxmLAAQ'
    WHEN frontend_vhost = 'tamarind.toucantoco.com' THEN '00109000017X5NLAA0'
    WHEN frontend_vhost = 'pinpoint.toucantoco.com' THEN '0010900000pf21RAAQ'
    WHEN frontend_vhost = 'remmelzwaan.toucantoco.com' THEN '00109000005fyr8AAA'
    WHEN frontend_vhost = 'rpa-h4h.toucantoco.com' THEN '00109000005g107AAA'
    WHEN frontend_vhost = 'axa-im.toucantoco.com' THEN '00109000005ftHvAAI'
    WHEN frontend_vhost = 'publishing-profits.toucantoco.com' THEN '00109000012CxSfAAK'
    WHEN frontend_vhost = 'quantmetry.toucantoco.com' THEN '00109000005g1dpAAA'
    WHEN frontend_vhost = 'ey-italy.toucantoco.com' THEN '00109000005ftjOAAQ'
    WHEN frontend_vhost = 'pricehubble.toucantoco.com	' THEN '00109000005ftJaAAI'
    WHEN frontend_vhost = 'decathlon.toucantoco.com' THEN '00109000005fushAAA'
    WHEN frontend_vhost = 'vo2.toucantoco.com' THEN '00109000005fs0VAAQ'
    WHEN frontend_vhost = 'mindsdb.toucantoco.com' THEN '001IV00000eFoGnYAK'
    WHEN frontend_vhost = 'nexusdemo.toucantoco.com' THEN '00109000005fxFSAAY'
    WHEN frontend_vhost = 'cstbgroup.toucantoco.com' THEN '00109000005fs2xAAA'
    WHEN frontend_vhost = 'idealis-consulting.toucantoco.com' THEN '001IV00000eFuEBYA0'
    WHEN frontend_vhost = 'interieur.toucantoco.com' THEN '00109000005frvSAAQ'
    WHEN frontend_vhost = 'oxatis.toucantoco.com' THEN '00109000005fyxvAAA'
    WHEN frontend_vhost = 'monnier-energies.toucantoco.com' THEN '0010900001HFAOvAAP'
    WHEN frontend_vhost = 'showagroup.toucantoco.com' THEN '00109000005fy38AAA'
    WHEN frontend_vhost = 'teamwill.toucantoco.com' THEN '00109000005fuk0AAA'

    ELSE b.account_id
    END AS account_id


FROM second_dim_instances_table a

LEFT JOIN instance_account_table b ON a.frontend_vhost = b.instance_url_domain


WHERE instance_usage = 'clients'

AND frontend_vhost NOT IN ('cegid-optim.toucantoco.guru','danone', 'sncf-retaildata', 'rcsuresnes.toucantoco.com', 'sandbox-prod-us.toucantoco.com', 'pre-francetravail', 'greencockpit.toucantoco.com',
'datarh-swissa.toucantoco.com', 'apitech.toucantoco.com', 'arval-test', 'dae.toucantoco.com', 'recommerce.toucantoco.com', 'ventacity-staging',
'pwc', 'solutions-public')


AND instance != 'a11ywatch.com'



