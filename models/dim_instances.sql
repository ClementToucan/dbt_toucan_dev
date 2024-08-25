{{ config(materialized='table')  }}


WITH toucan_instances_table AS (

SELECT

DISTINCT


    instance


FROM mixpanel.raw__mp_master_event

WHERE DATE(time) >= DATE('2022-01-01')


),


shaun_instance_deduplicated AS (

SELECT


    name,

    toucan_version,

    instance_type,

    disabled,

    analytics_flag,

    embed_flag




FROM prod_config_analytics_v3.shaun_instances


GROUP BY 1,2,3,4,5,6


),

final_table AS (

SELECT

    CASE

    WHEN instance_type = 'Customers' THEN 'clients'

    WHEN instance_type = 'Tokars' THEN 'internal'
    WHEN a.instance LIKE '%toucantoco.guru%' THEN 'internal'
    WHEN a.instance  LIKE 'toucantoco.dev' THEN 'internal'
    WHEN a.instance LIKE '%frontend-layout-service%' THEN 'internal'
    WHEN a.instance  LIKE '%frontend.impersonate%' THEN 'internal'
    WHEN a.instance LIKE '%frontend-impersonate%' THEN 'internal'
    WHEN a.instance LIKE '%toucantoco.guru%' THEN 'internal'
    WHEN a.instance LIKE '%toucantoco.dev%' THEN 'internal'
    WHEN a.instance IN ('localhost:8000', 'demo-staging', 'frontend-dev-env-test', 'sandbox-test-move', 'demo-staging-public', 'marketing', 'demo-staging.', 'sandbox-prod-us', 'sandbox-test-move-duplicate',
    'localhost:8000', 'demo-staging', 'training-v3', 'frontend-dev-env-test', 'sandbox-test-move', 'demo-staging-public', 'demo-embed-public', 'public') THEN 'internal'

    WHEN a.instance IN ('public', 'training-v3', 'demo-embed-public', 'training-2020', 'training-2021', 'aws-demo') THEN 'demo'

    ELSE 'clients'

    END AS instance_usage,

    CASE 
    WHEN toucan_version = 'LTS' THEN 'V2'
    WHEN toucan_version = 'V3' THEN 'V3'
    WHEN a.instance LIKE '%v3%' THEN 'V3'
    ELSE 'Unknown'
    END AS toucan_version,

    a.instance,

    CASE

    WHEN a.instance IN ('atout-france', 'atout-france-2') THEN 'Atout France'
    WHEN a.instance IN ('admin-myss-safe-streaming') THEN 'Safe Streaming'
     WHEN a.instance IN ('bebook-lts','bebook') THEN 'bebook'
    WHEN a.instance IN ('bnpp-wm', 'arval-test', 'arval', 'arval-preprod', 'arval-migration-v3', 'arval-v3') THEN 'BNP Paribas'
    WHEN a.instance IN ('ca-cib', 'ca-sa', 'cats-centre-est', 'cats', 'cats-loire-haute-loire.', 'ca-alpes-provence', 'ca-aquitaine', 'ca-bp','ca-bp-v3', 'ca-lorraine', 
    'ca-indosuez', 'ca-nord-est', 'ca-normandie-seine', 'cadif', 'capg', 'carcentre', 'cats-catalogue-apps', 'cats-centre-france', 'cats-loire-haute-loire',
    'cats-preprod', 'ca-mobility') THEN  'Crédit Agricole'
    WHEN a.instance IN ('capgemini', 'capgemini-ind', 'quantmetry','solution-capgemini') THEN 'Capgemini'
    WHEN a.instance IN ('engiesolutionstp','engie', 'engiesolutionstp-preprod') THEN 'Engie'
    WHEN a.instance = 'ensae-dataviz' THEN 'ENSAE'
    WHEN a.instance = 'eosspacelink' THEN 'SpaceLink'
    WHEN a.instance = 'eztexting' THEN 'EZ Texting'
    WHEN a.instance = 'blogdumoderateur' THEN 'BDM'
    WHEN a.instance = 'falconbi' THEN 'Falcon Business Intelligence'
    WHEN a.instance = 'finances' THEN 'Direction Générale Des Finances Publiques'
    WHEN a.instance IN ('gares-et-connexions', 'sncf', 'sncf-innovation', 'sncf-retaildata', 'sncf-retaildata', 'sncf-telecom') THEN 'SNCF'
    WHEN a.instance IN ('hauts-de-seine-habitat','happi') THEN 'Hauts-de-Seine Habitat'
    WHEN a.instance = 'ieseg' THEN 'IESEG'
    WHEN a.instance = 'icl' THEN 'Université Catholique de Lille'
    WHEN a.instance = 'ieif' THEN "Institut de L'Epargne Immobilière & Foncière"
    WHEN a.instance = 'belharra' THEN 'Belharra Numérique'
    WHEN a.instance = 'iessynergy' THEN 'IES Synergy'
    WHEN a.instance = 'jems-group' THEN 'JEMS Group'
    WHEN a.instance IN ('kinsa','kinsa-staging') THEN 'Kinsa Inc.'
    WHEN a.instance = 'lineup' THEN 'Lineup.ai'
    WHEN a.instance = 'littlebigconnection' THEN 'LittleBig Connection'
    WHEN a.instance = 'namr' THEN 'NamR'
    WHEN a.instance = 'ofii' THEN "Office Français de l'Immigration et de l'Intégration"
    WHEN a.instance = 'oncf' THEN "L'Office National des Chemins de Fer"
    WHEN a.instance = 'brightfunds' THEN 'Bright Funds'
    WHEN a.instance  = 'carbone4' THEN 'Carbone 4'
    WHEN a.instance = 'cluster-maritime' THEN 'Cluster Maritime Français'
    WHEN a.instance IN ('connectedcircles','engage.connectedcircles.net') THEN 'Connected Circles'
    WHEN a.instance = 'ctm' THEN 'Collectivité Territoriale de Martinique'
    WHEN a.instance = 'dataneo-portail' THEN 'Dataneo'
    WHEN a.instance = 'dcpower.cc' THEN 'DC Power Solutions'
    WHEN a.instance = 'dfakto' THEN 'dFakto'
    WHEN a.instance IN ('diac', 'rci-finance-maroc', 'rfi') THEN 'Renault Group'
    WHEN a.instance = 'exelcia' THEN 'Exelcia IT'
    WHEN a.instance = 'gisxstudios' THEN 'UBI Global'
    WHEN a.instance = 'greenplatform' THEN 'TNP Consultants'
    WHEN a.instance = 'idemaps' THEN 'iDemaps'
    WHEN a.instance = 'insideboard' THEN 'HAFA'
    WHEN a.instance = 'itinsell' THEN 'ITinSell'
    WHEN a.instance = 'nextdecision' THEN 'Next Decision'
    WHEN a.instance IN ('upstreampay','purse-v3', 'upstreampay-v3') THEN 'Purse'
    WHEN a.instance = '360suite' THEN 'Wiiisdom'
    WHEN a.instance = '7ea3-2a01-cb0c-978-9e00-c4e8-d710-974d-6b55.ngrok-free.app' THEN 'ngrok'
    WHEN a.instance = 'a11ywatch.com' THEN 'A11yWatch'
    WHEN a.instance IN ('alcopa--auction-toucantoco-com.translate.goog', 'alcopa-auction', 'alcopa-auction-v3') THEN 'Alcopa Auction'
    WHEN a.instance IN ('verifiedmarketresearch.com','verifiedmarketresearch') THEN 'Verified Market Research'
    WHEN a.instance = 'app.keiz.ai' THEN 'Keiz.ai'
    WHEN a.instance = 'apps.odasa.com.au' THEN 'The Office for Design and Architecture South Australia'
    WHEN a.instance IN ('barometre-lpm.defense.gouv.fr', 'barometre-minarm','combattantes-numerique', 'ministere-des-armees') THEN 'Ministère des Armées'
    WHEN a.instance IN ('bilan.programme-cee-actee.fr','fnccr') THEN 'Fédération Nationale des Collectivités Concédantes et Régies (FNCCR)'
    WHEN a.instance = 'cervecerialatropical' THEN 'Cerveceria La Tropical'
    WHEN a.instance IN ('cgi', 'gci.', 'cgi-v3') THEN 'CGI'
    WHEN a.instance IN ('cstbgroup','cstb') THEN 'Centre Scientifique et Technique du Bâtiment'
    WHEN a.instance IN ('dashboard-lengo-ai.translate.goog', 'dashboard.lengo.ai') THEN 'lengo.ai'
    WHEN a.instance IN ('dashboard.eurazeo.com', 'eurazeo', 'eurazeo-data') THEN 'Eurazeo'
    WHEN a.instance = 'dashboard.impact-plus.fr' THEN 'Impact+'
    WHEN a.instance IN ('data-territoire-public', 'www.data-territoire.fr') THEN 'Ministère de la Transformation et de la Fonction Publiques'
    WHEN a.instance IN ('datavision-economie-gouv-fr.translate.goog', 'datavision.economie.gouv.fr') THEN "Ministère de l'Économie, des Finances et de la Souveraineté Industrielle et Numérique"
    WHEN a.instance = 'ditp' THEN 'Direction Interministérielle de la Transformation Publique (DITP)'
    WHEN a.instance IN ('france-travail', 'francetravail', 'pre-francetravail', 'pole-emploi', 'pre-pole-emploi') THEN 'France Travail'
    WHEN a.instance = 'francedigitale' THEN 'France Digitale'
    WHEN a.instance IN ('gouvernance.edicia.fr','edicia') THEN 'Edicia'
    WHEN a.instance = 'granbury' THEN 'Granbury Solutions'
    WHEN a.instance IN ('h4h','rpa-h4h') THEN 'H4H'
    WHEN a.instance = 'hdb' THEN 'Hauts-de-Bièvre Habitat'
    WHEN a.instance IN ('hector-forvia','faurecia') THEN 'FORVIA'
    WHEN a.instance = 'interfas-display' THEN 'INTERFAS'
    WHEN a.instance = 'issforphilips' THEN 'ISS World'
    WHEN a.instance = 'italiadelleimprese.eulerhermes.com' THEN 'Italia delle Imprese'
    WHEN a.instance = 'lifeconnectionofohio'  THEN 'Life Connection of Ohio'
    WHEN a.instance IN ('mindyourreceivables.allianz-trade.com', 'mindyourreceivables.eulerhermes.com', 'euler-hermes', 'tradematch', 'tradematch.allianz-trade.com', 'tradematch.eulerhermes.com') THEN 'Allianz Trade'
    WHEN a.instance IN ('mobilizepowersolutions','mobilize-ch') THEN 'Mobilize'
    WHEN a.instance = 'momentslab' THEN 'Moments Lab'
    WHEN a.instance IN ('monnie-ennergies', 'monnier-energies', 'positvimpact.vinci.net', 'positv-vinci') THEN 'Vinci'
    WHEN a.instance IN ('mydata','exponens', 'exponens-v3') THEN 'Exponens'
    WHEN a.instance = 'nanoblue' THEN 'Nano Blue'
    WHEN a.instance = 'newseason' THEN 'New Season'
    WHEN a.instance = 'nexity-sp' THEN 'Nexity'
    WHEN a.instance = 'nouvellesdonnes' THEN 'Nouvelles Donnes'
    WHEN a.instance IN ('observatoire.qualiteconstruction.com','observatoire-aqc') THEN 'Agence Qualité Construction'
    WHEN a.instance = 'ogf' THEN 'OGF'
    WHEN a.instance IN ('portail-contrat.ratp.fr', 'ratp') THEN 'RATP'
    WHEN a.instance IN ('procivis-toucantoco-com.translate.goog', 'procivis', 'procivis') THEN 'procivis'
    WHEN a.instance IN ('pwc') THEN 'PwC'
    WHEN a.instance IN ('robomat-preprod','robomat') THEN 'Littoral Normand'
    WHEN a.instance = 'rugby' THEN 'XV de France'
    WHEN a.instance = 'scc' THEN 'SCC-LSGI'
    WHEN a.instance = 'sesamlld.dataneo.fr' THEN 'Sesamlld'
    WHEN a.instance = 'showagroup' THEN 'Showa Group'
    WHEN a.instance = 'solutions-public' THEN 'Solutions Public'
    WHEN a.instance IN ('stellantis-owned-retail','psa-banque', 'stellantis', 'swp') THEN 'Stellantis'
    WHEN a.instance IN ('stratumn', 'stratumn-trace') THEN 'Sia Partners'
    WHEN a.instance IN ('sycodes.qualiteconstruction.com') THEN 'Agence Qualité Construction'
    WHEN a.instance IN ('tamarind') THEN 'Tamarind Intelligence'
    WHEN a.instance IN ('te46-public','te46') THEN "Territoire d'Energie Lot (TE46)"
    WHEN a.instance IN ('territoires.atdalsace.eu','adauhr') THEN 'ADAUHR-ATD Alsace'
    WHEN a.instance = 'tf1' THEN 'TF1'
    WHEN a.instance = 'tnevision' THEN 'TnE Vision'
    WHEN a.instance = 'totalenergies' THEN 'TotalEnergies'
    WHEN a.instance IN ('ventacity', 'ventacity-staging') THEN 'Ventacity Systems'
    WHEN a.instance = 'wave.webaim.org' THEN 'WAVE Web Accessibility Evaluation Tools'
    WHEN a.instance IN ('willaman.', 'willaman', 'willaman-v3') THEN 'Willaman'
    WHEN a.instance = 'wynd' THEN 'Anycommerce by ChapsVision'
    WHEN a.instance IN ('aerm','aerm-v3') THEN "L’Agence de L’Eau Rhin-Meuse"
    WHEN a.instance = 'axa-im' THEN 'Axa'
    WHEN a.instance = 'banque-des-territoires' THEN 'Caisse des Dépôts et Consignations'
    WHEN a.instance = 'cfdt' THEN 'CFDT'
    WHEN a.instance = 'cfg' THEN 'Certified Financial Group Inc.'
    WHEN a.instance IN ('claridgeproducts','claridgeproducts-v3') THEN 'Claridge Products'
    WHEN a.instance IN ('cma-cgm','pre-cma-cgm') THEN 'CMA CGM'
    WHEN a.instance = 'codata' THEN 'CODATA'
    WHEN a.instance = 'cost-housemaroc' THEN 'Cost House'
    WHEN a.instance = 'dae' THEN 'DAE'
    WHEN a.instance IN ('deloitte-uk', 'deloitte') THEN 'Deloitte'
    WHEN a.instance IN ('dmgroupk12') THEN 'District Management Group'
    WHEN a.instance = 'dne-bourgognefranchecomte' THEN 'Région Bourgogne-Franche-Comté'
    WHEN a.instance = 'douanes' THEN 'Direction Générale des Douanes et Droits Indirects'
    WHEN a.instance = 'edenhotels' THEN 'Eden Hotels'
    WHEN a.instance IN ('ems','ems-v3') THEN 'Elite Medical Staffing'
    WHEN a.instance = 'eu4ua' THEN 'EU4UA'
    WHEN a.instance IN ('eurus-preprod') THEN 'Eurus'
    WHEN a.instance = 'evolution-international' THEN 'Synetics'
    WHEN a.instance IN ('ey-italy') THEN 'EY'
    WHEN a.instance IN ('fedom') THEN "Fédération des Entreprises des Outre-Mer"
    WHEN a.instance IN ('ffbatiment') THEN 'Fédération Française du Bâtiment'
    WHEN a.instance = 'firstchoicehealthcare' THEN '1st Choice Healthcare'
    WHEN a.instance = 'footanalytics' THEN 'Foot Analytics'
    WHEN a.instance = 'georgetowncghpi' THEN 'Georgetown University'
    WHEN a.instance = 'groupebms' THEN 'Groupe BMS'
    WHEN a.instance = 'iaudio' THEN 'ioAudio'
    WHEN a.instance IN ('idemaps-demo', 'idemaps360') THEN 'iDemaps'
    WHEN a.instance = 'interieur' THEN "Ministère De L'Intérieur"
    WHEN a.instance = 'intoanalytics' THEN 'IntoAnalytics'
    WHEN a.instance = 'ipsen-meam' THEN 'Ipsen'
    WHEN a.instance IN ('iut', 'iut-v3') THEN 'Instituts Universitaires de Technologie'
    WHEN a.instance IN ('ivyresearchcouncil', 'ivyresearchcouncil-staging') THEN 'Ivy Research Council'
    WHEN a.instance = 'jouve' THEN 'Luminess'
    WHEN a.instance = 'jubholland' THEN 'JUB Holland'
    WHEN a.instance = 'lacompagniedusav' THEN 'La Compagne du SAV'
    WHEN a.instance = 'ldz' THEN 'Carrefour'
    WHEN a.instance = 'leroy-somer' THEN 'Nidec Leroy-Somer'
    WHEN a.instance = 'lookey' THEN "L'académie du Service"
    WHEN a.instance IN ('loreal','loreal-digital') THEN "L'Oréal"
    WHEN a.instance = 'maif' THEN 'MAIF'
    WHEN a.instance = 'mcr-consultants' THEN 'MCR-Consultants'
    WHEN a.instance = 'mdesserts' THEN 'Mademoiselles Desserts (Emmi)'
    WHEN a.instance = 'milestones' THEN 'Milestones.Tech'
    WHEN a.instance = 'mind7' THEN 'Mind7 Consulting'
    WHEN a.instance = 'mindsdb' THEN 'MindsDB'
    WHEN a.instance = 'mms' THEN 'Mountain Motorsports'
    WHEN a.instance = 'moet-hennessy' THEN 'Moët Hennessy'
    WHEN a.instance IN ('monbsinexity','nexity') THEN 'Nexity'
    WHEN a.instance = 'mongodb' THEN 'MangoDB'
    WHEN a.instance = 'monparcours-icl' THEN 'ICL'
    WHEN a.instance = 'mtes' THEN 'Ministère de la Transition Ecologique et Solidaire'
    WHEN a.instance = 'myrg' THEN 'Groupe RG'
    WHEN a.instance = 'nextsise' THEN 'Cabinet Premier Ministre'
    WHEN a.instance = 'oddo-bhf' THEN 'ODDO BHF'
    WHEN a.instance = 'praxedo-preprod' THEN 'Praxedo'
    WHEN a.instance IN ('preprod-smart-tribune','smart-tribune-v3') THEN 'Smart Tribune'
    WHEN a.instance = 'pricinghub-preprod' THEN 'PricingHUB'
    WHEN a.instance = 'pwdfinances' THEN 'PWD FINANCES'
    WHEN a.instance = 'rcsuresnes' THEN 'RC Suresnes'
    WHEN a.instance = 'remmelzwaan' THEN 'Planalyse'
    WHEN a.instance = 'retailcompliance' THEN 'Nissan'
    WHEN a.instance = 'sabenatechnics' THEN 'Sabena Technics'
    WHEN a.instance = 'sacd' THEN 'Société des Auteurs et Compositeurs Dramatiques (SACD)'
    WHEN a.instance = 'saegus' THEN 'Saegus'
    WHEN a.instance IN ('saintlaurent', 'saintlaurent-test') THEN 'Yves Saint Laurent'
    WHEN a.instance = 'serene' THEN 'Serene Network'
    WHEN a.instance = 'siamu' THEN 'SIAMU'
    WHEN a.instance = 'skconsulting' THEN 'SK Consulting'
    WHEN a.instance = 'skf' THEN 'SKF'
    WHEN a.instance = 'team-group-public' THEN 'Team-Group'
    WHEN a.instance = 'theotherstore' THEN 'The Other Store'
    WHEN a.instance = 'umanis-onboarding' THEN 'Umanis'
    WHEN a.instance = 'usaautomotivepartners'  THEN 'USA Automotive Partners' 
    WHEN a.instance = 'areas-v3' THEN 'Areas'
    WHEN a.instance = 'axialys-v3' THEN 'Axialys'
    WHEN a.instance = 'cerfrancebretagne-v3' THEN 'Cerfrance'
    WHEN a.instance = 'cleyrop-v3' THEN 'Cleyrop'
    WHEN a.instance = 'combohr-v3' THEN 'Combo'
    WHEN a.instance = 'dfakto-v3' THEN 'dFakto'
    WHEN a.instance = 'elior-v3' THEN 'Elior'
    WHEN a.instance = 'esgecoonline' THEN 'EcoOnline'
    WHEN a.instance = 'groupe-adelaide-v3' THEN 'Groupe Adelaïde'
    WHEN a.instance = 'jacobs' THEN 'Jacobs'
    WHEN a.instance = 'klarahr' THEN 'Klara'
    WHEN a.instance = 'knave-v3' THEN 'Knave'
    WHEN a.instance = 'newsbridge' THEN 'Moments Lab'
    WHEN a.instance = 'portail-dataneo' THEN 'Dataneo'
    WHEN a.instance = 'springly' THEN 'Springly'
    WHEN a.instance = 'strada-v3' THEN 'strada'
    WHEN a.instance = 'synergy-v3' THEN 'Synergy'
    WHEN a.instance = 'universite-evry-paris-saclay' THEN 'Université Évry Paris-Saclay'
    WHEN a.instance = 'voysen-v3' THEN 'voysen'
    WHEN a.instance = 'ampd-research' THEN 'AMPD Research'

    ELSE INITCAP(a.instance)

    END AS client,


    CASE

    WHEN a.instance IN ('areas','endalia', 'tamarind', 'footanalytics', 'triskell-software', 'xceed', 'areas-v3') THEN 'Spain'

    WHEN a.instance IN ('civical','biotest') THEN 'Germany'

    WHEN a.instance = 'ecometrica' THEN 'Scotland'

    WHEN a.instance IN ('enel', 'wonderstore', 'italiadelleimprese.eulerhermes.com') THEN 'Italy'

    WHEN a.instance IN ('xoomworks', 'astrazeneca', 'clubspark','esgecoonline') THEN 'England'

    WHEN a.instance IN ('bebook-lts','bebook', 'connectedcircles', 'hyrde', 'engage.connectedcircles.net', 'eteck', 'intodata', 'showagroup', 'edenhotels', 'intoanalytics','JUB Holland') THEN 'Netherlands'

    WHEN a.instance IN ('gisxstudios', 'skf') THEN 'Sweden'

    WHEN a.instance IN ('lipscore') THEN 'Norway'

    WHEN a.instance IN ('issforphilips', 'leo-pharma') THEN 'Denmark'

    WHEN a.instance IN ('kaspard', 'aremis', 'idealis-consulting', 'siamu') THEN 'Belgium'

    WHEN a.instance = 'pricehubble' THEN 'Switzerland'

    WHEN a.instance IN ('dashboard-lengo-ai.translate.goog', 'dashboard.lengo.ai', 'lengo') THEN 'Sénégal'

    WHEN a.instance = 'kanari' THEN 'United Arab Emirates'

    WHEN a.instance = 'retailcompliance' THEN 'Japan'

    WHEN a.instance = 'ampd-research' THEN 'Singapore'

    WHEN a.instance IN ('pinpoint', 'synergiq', 'cgi', 'gci.', 'cgi-v3') THEN 'Canada'

    WHEN a.instance IN ('safegroup','apps.odasa.com.au', 'nanoblue') THEN 'Australia'

    WHEN a.instance IN ('accenture','archondev', 'brightfunds', 'companykitchen', 'corecentra', 'dcpower.cc', 'eosspacelink', 'eztexting', 'haptik', 'harrisx', 'kinsa', 'kinsa-staging', 'lineup',
    'omnicell', 'photoshelter', 'pourtastic', 'storyslab', 'team-group','team-group-public', 'theventurelane', 'thryv', 'willaman','willaman-v3','7ea3-2a01-cb0c-978-9e00-c4e8-d710-974d-6b55.ngrok-free.app',
    'a11ywatch.com', 'verifiedmarketresearch.com', 'cervecerialatropical', 'coty', 'github', 'granbury', 'interlace-health', 'newseason', 'optiisolutions', 'PwC', 'redi-view.app', 'truelytics',
    'ventacity', 'ventacity-staging', 'verifiedmarketresearch', 'verisinsights', 'visionweb', 'wave.webaim.org', 'willaman.', '247software', 'cfg','claridgeproducts','claridgeproducts-v3', 'deloitte', 'deloitte-uk',
    'dmgroupk12','dmgroupk12-v3', 'elitemedicalstaffing', 'ey-italy', 'firstchoicehealthcare', 'georgetowncghpi', 'hometap', 'ioaudio.io', 'ivyresearchcouncil', 'ivyresearchcouncil-staging', 'mindsdb', 'mms', 'slalom', 'usaautomotivepartners',
    'alteryx','clyr', 'jacobs')  THEN  'USA'

    WHEN a.instance = 'tecmart' THEN 'Guatemala'

    ELSE 'France'

    END AS client_country,

    CASE
    WHEN disabled IS TRUE THEN 'disabled'
    WHEN disabled IS FALSE THEN 'active'
    ELSE 'unknown'
    END AS instance_status,

    CASE 
    WHEN analytics_flag IS NULL THEN 'unknown'
    ELSE analytics_flag 
    END AS analytics_flag,

    CASE 
    WHEN embed_flag IS NULL THEN 'unknown'
    ELSE embed_flag 
    END AS embed_flag




FROM toucan_instances_table a

LEFT JOIN shaun_instance_deduplicated b ON b.name = a.instance

WHERE a.instance IS NOT NULL

ORDER BY 1,2,3,4


)

SELECT

    *,

    CASE
    WHEN client_country IN ('France','Italy','Berlgium', 'Spain', 'Germany', 'England', 'Scotland', 'Netherlands','Denmark', 'Sweden', 'Switzerland','Norway') THEN 'Europe'
    WHEN client_country = 'Sénégal' THEN 'Africa'
    WHEN client_country IN ('Australia', 'Japan', 'Singapore') THEN 'APAC'
    WHEN client_country IN ('United Arab Emirates') THEN 'Middle-East'
    WHEN client_country IN ('Canada', 'USA', 'Guatemala') THEN 'America'
    ELSE 'Other'
    END AS client_region

FROM final_table

