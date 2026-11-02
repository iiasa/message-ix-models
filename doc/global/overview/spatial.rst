.. _spatial:

Regions
*******

|name| is operated in a variety of spatial scopes and resolutions,
but most commonly with global scope
and 12 regions (:ref:`R12 <R12>`), each including one or more countries
(see :numref:`fig-reg` and :numref:`tab-reg` below).
GLOBIOM has a native 59-region resolution,
aggregated to the |name| regions in the linkage,
and GAINS a native resolution of 182 regions,
aggregated in the same way.

.. _fig-reg:
.. figure:: /_static/MESSAGE_regions.png
   :width: 800px

   Map of 12 |name| regions.

The country definitions of the 12 |name| regions are described in the table
below (:numref:`tab-reg`).
In alternate spatial aggregations, one or more countries are separated from
these regions, giving the :ref:`R14 <R14>`, :ref:`R17 <R17>` and
:ref:`R20 <R20>` variants, while :ref:`R11 <R11>` preceded R12 and does not
separate China from the rest of Centrally Planned Asia.
The code lists behind every aggregation, including the country assignments
reproduced below, are at :doc:`/pkg-data/node`.

.. _tab-reg:
.. list-table:: The 12 regions of |name| and their country definitions.
   :widths: 8 18 74
   :header-rows: 1

   * - Region
     - Definition
     - Countries
   * - **NAM**
     - North America
     - Canada, Guam, Puerto Rico, Saint Pierre and Miquelon, United States, United States Virgin Islands
   * - **LAM**
     - Latin America and The Caribbean
     - Anguilla, Antigua and Barbuda, Argentina, Aruba, Bahamas, Barbados, Belize, Bermuda, Bolivia, Bonaire, Sint Eustatius and Saba, Brazil, British Virgin Islands, Cayman Islands, Chile, Colombia, Costa Rica, Cuba, Curaçao, Dominica, Dominican Republic, Ecuador, El Salvador, Falkland Islands, French Guiana, Grenada, Guadeloupe, Guatemala, Guyana, Haiti, Honduras, Jamaica, Martinique, Mexico, Montserrat, Netherlands Antilles, Nicaragua, Panama, Paraguay, Peru, Saint Kitts and Nevis, Saint Lucia, Saint Vincent and the Grenadines, Sint Maarten (Dutch part), Suriname, Trinidad and Tobago, Turks and Caicos Islands, Uruguay, Venezuela
   * - **WEU**
     - Western Europe
     - Andorra, Austria, Belgium, Cyprus, Denmark, Faroe Islands, Finland, France, Germany, Gibraltar, Greece, Greenland, Iceland, Ireland, Isle of Man, Italy, Liechtenstein, Luxembourg, Malta, Monaco, Netherlands, Norway, Portugal, San Marino, Spain, Svalbard and Jan Mayen, Sweden, Switzerland, Turkey, United Kingdom, Vatican City
   * - **EEU**
     - Central and Eastern Europe
     - Albania, Bosnia and Herzegovina, Bulgaria, Croatia, Czechia, Estonia, Hungary, Latvia, Lithuania, Montenegro, North Macedonia, Poland, Romania, Serbia, Serbia and Montenegro, Slovakia, Slovenia, Yugoslavia
   * - **FSU**
     - Former Soviet Union
     - Armenia, Azerbaijan, Belarus, Georgia, Kazakhstan, Kyrgyzstan, Republic of Moldova, Russian Federation, Tajikistan, Turkmenistan, Ukraine, Uzbekistan
   * - **MEA**
     - Middle East and North Africa
     - Algeria, Bahrain, Egypt, Iran, Iraq, Israel, Jordan, Kuwait, Lebanon, Libya, Morocco, Oman, Palestine, Qatar, Saudi Arabia, South Sudan, Sudan, Syria, Tunisia, United Arab Emirates, Western Sahara, Yemen
   * - **AFR**
     - Sub-Saharan Africa
     - Angola, Benin, Botswana, Burkina Faso, Burundi, Cabo Verde, Cameroon, Central African Republic, Chad, Comoros, Côte d'Ivoire, Democratic Republic of the Congo, Djibouti, Equatorial Guinea, Eritrea, Eswatini, Ethiopia, Gabon, Gambia, Ghana, Guinea, Guinea-Bissau, Kenya, Lesotho, Liberia, Madagascar, Malawi, Mali, Mauritania, Mauritius, Mayotte, Mozambique, Namibia, Niger, Nigeria, Republic of the Congo, Rwanda, Réunion, Saint Helena, Sao Tome and Principe, Senegal, Seychelles, Sierra Leone, Somalia, South Africa, Tanzania, Togo, Uganda, Zambia, Zimbabwe
   * - **SAS**
     - South Asia
     - Afghanistan, Bangladesh, Bhutan, India, Maldives, Nepal, Pakistan, Sri Lanka
   * - **CHN**
     - China
     - China, Hong Kong
   * - **RCPA**
     - Rest Centrally Planned Asia
     - Cambodia, Democratic People's Republic of Korea, Lao People's Democratic Republic, Mongolia, Vietnam
   * - **PAS**
     - Other Pacific Asia
     - American Samoa, Brunei Darussalam, Christmas Island, Cocos (Keeling) Islands, Cook Islands, Fiji, French Polynesia, Indonesia, Kiribati, Macao, Malaysia, Marshall Islands, Micronesia, Myanmar, Nauru, New Caledonia, Niue, Norfolk Island, Northern Mariana Islands, Pacific Islands (Trust Territory), Palau, Papua New Guinea, Philippines, Pitcairn, Republic of Korea, Samoa, Singapore, Solomon Islands, Taiwan, Thailand, Timor-Leste, Tokelau, Tonga, Tuvalu, Vanuatu, Wallis and Futuna
   * - **PAO**
     - Pacific OECD
     - Australia, Japan, New Zealand

In addition to the 12 geographical regions,
|name| carries a global trade region, ``R12_GLB``,
where global energy markets clear
and where international shipping bunker fuel demand,
uranium resource extraction
and the nuclear fuel cycle are represented.

GLOBIOM has a native resolution of 59 regions,
which are aggregated to the |name| regions in the linkage
(:numref:`tab-globiomreg`).
The 180 countries GLOBIOM resolves are listed there.
Countries outside that set carry no land-use representation
and appear only in the energy system.

.. _tab-globiomreg:
.. list-table:: The 59 native regions of GLOBIOM and their aggregation to the regions of |name|.
   :widths: 8 20 72
   :header-rows: 1

   * - |name| region
     - GLOBIOM region
     - Countries
   * - **NAM**
     - CanadaReg
     - Canada
   * -
     - USAReg
     - Puerto Rico, United States
   * - **LAM**
     - ArgentinaReg
     - Argentina
   * -
     - BrazilReg
     - Brazil
   * -
     - MexicoReg
     - Mexico
   * -
     - RCAM
     - Bahamas, Belize, Costa Rica, Cuba, Dominican Republic, El Salvador, Guadeloupe, Guatemala, Haiti, Honduras, Jamaica, Nicaragua, Panama, Trinidad and Tobago
   * -
     - RSAM
     - Bolivia, Chile, Colombia, Ecuador, Falkland Islands, French Guiana, Guyana, Paraguay, Peru, Suriname, Uruguay, Venezuela
   * - **WEU**
     - AustriaReg
     - Austria
   * -
     - BelgiumReg
     - Belgium
   * -
     - CyprusReg
     - Cyprus
   * -
     - DenmarkReg
     - Denmark
   * -
     - FinlandReg
     - Finland
   * -
     - FranceReg
     - France
   * -
     - GermanyReg
     - Germany
   * -
     - GreeceReg
     - Greece
   * -
     - IrelandReg
     - Ireland
   * -
     - ItalyReg
     - Italy
   * -
     - LuxembourgReg
     - Luxembourg
   * -
     - MaltaReg
     - Malta
   * -
     - NetherlandsReg
     - Netherlands
   * -
     - PortugalReg
     - Portugal
   * -
     - ROWE
     - Greenland, Iceland, Norway, Switzerland, United Kingdom
   * -
     - SpainReg
     - Spain
   * -
     - SwedenReg
     - Sweden
   * -
     - TurkeyReg
     - Turkey
   * - **EEU**
     - BulgariaReg
     - Bulgaria
   * -
     - CroatiaReg
     - Croatia
   * -
     - CzechRepReg
     - Czech Republic
   * -
     - HungaryReg
     - Hungary
   * -
     - PolandReg
     - Poland
   * -
     - RCEU
     - Albania, Bosnia and Herzegovina, Montenegro, North Macedonia, Serbia
   * -
     - RomaniaReg
     - Romania
   * -
     - SlovakiaReg
     - Slovakia
   * -
     - SloveniaReg
     - Slovenia
   * - **FSU**
     - EstoniaReg
     - Estonia
   * -
     - Former USSR
     - Armenia, Azerbaijan, Belarus, Georgia, Kazakhstan, Kyrgyzstan, Republic of Moldova, Tajikistan, Turkmenistan, Uzbekistan
   * -
     - LatviaReg
     - Latvia
   * -
     - LithuaniaReg
     - Lithuania
   * -
     - RussiaReg
     - Russian Federation
   * -
     - UkraineReg
     - Ukraine
   * - **MEA**
     - MiddleEast
     - Bahrain, Iran, Iraq, Israel, Jordan, Kuwait, Lebanon, Oman, Palestine, Qatar, Saudi Arabia, Syria, United Arab Emirates, Yemen
   * -
     - NorthernAf
     - Algeria, Egypt, Libya, Morocco, Tunisia, Western Sahara
   * - **AFR**
     - CongoBasin
     - Cameroon, Central African Republic, Democratic Republic of the Congo, Equatorial Guinea, Gabon, Republic of the Congo
   * -
     - EasternAf
     - Burundi, Ethiopia, Kenya, Rwanda, Tanzania, Uganda
   * -
     - SouthAfrReg
     - South Africa
   * -
     - SouthernAf
     - Angola, Botswana, Comoros, Eswatini, Lesotho, Madagascar, Malawi, Mauritius, Mozambique, Namibia, Réunion, Zambia, Zimbabwe
   * -
     - WesternAf
     - Benin, Burkina Faso, Cape Verde, Chad, Côte d'Ivoire, Djibouti, Eritrea, Gambia, Ghana, Guinea, Guinea-Bissau, Liberia, Mali, Mauritania, Niger, Nigeria, Senegal, Sierra Leone, Somalia, Sudan, Togo
   * - **SAS**
     - IndiaReg
     - India
   * -
     - RSAS
     - Bangladesh, Bhutan, Nepal, Pakistan, Sri Lanka
   * - **CHN**
     - ChinaReg
     - China
   * - **RCPA**
     - RSEA PAC
     - Cambodia, Democratic People's Republic of Korea, Lao People's Democratic Republic, Mongolia, Vietnam
   * - **PAS**
     - IndonesiaReg
     - Indonesia
   * -
     - MalaysiaReg
     - Malaysia
   * -
     - Pacific Islands
     - Fiji, French Polynesia, New Caledonia, Papua New Guinea, Samoa, Solomon Islands, Vanuatu
   * -
     - RSEA OPA
     - Brunei Darussalam, Myanmar, Philippines, Singapore, Thailand, Timor-Leste
   * -
     - SouthKorea
     - Republic of Korea
   * - **PAO**
     - AustraliaReg
     - Australia
   * -
     - JapanReg
     - Japan
   * -
     - NewZealandReg
     - New Zealand

GAINS works at a native resolution of 182 regions
and is aggregated to the twelve regions as shown below (:numref:`tab-gainsreg`).

.. _tab-gainsreg:
.. list-table:: The 182 native regions of GAINS and their aggregation to the regions of |name|.
   A count in brackets gives the number of countries an aggregate GAINS region covers.
   Where GAINS resolves a country below the national level, the country is named
   with the number of sub-national regions it carries.
   :widths: 8 8 84
   :header-rows: 1

   * - |name| region
     - GAINS regions
     - Native GAINS regions
   * - **NAM**
     - 4
     - CANA_WHOL (Canada), CARB_WHOL (18 countries), USAM_ALAS (United States of America, 2 regions), USAM_MAIN (United States of America, 2 regions)
   * - **LAM**
     - 13
     - ARGE_WHOL (Argentina), BOLV_WHOL (Bolivia (Plurinational State of)), BRAZ_WHOL (Brazil), CARB_WHOL (18 countries), CEAM_WHOL (7 countries), CHIL_WHOL (Chile), COLO_WHOL (Colombia), ECUA_WHOL (Ecuador), MEXI_WHOL (Mexico), PARA_WHOL (Paraguay), PERU_WHOL (Peru), URUG_WHOL (Uruguay), VENE_WHOL (Venezuela (Bolivarian Republic of))
   * - **WEU**
     - 21
     - AUST_WHOL (Austria), BELG_WHOL (Belgium), CYPR_WHOL (Cyprus), DENM_WHOL (Denmark), FINL_WHOL (Finland), FRAN_WHOL (France), GERM_WHOL (Germany), GREE_WHOL (Greece), ICEL_WHOL (Iceland), IREL_WHOL (Ireland), ITAL_WHOL (Italy), LUXE_WHOL (Luxembourg), MALT_WHOL (Malta), NETH_WHOL (Netherlands), NORW_WHOL (Norway), PORT_WHOL (Portugal), SPAI_WHOL (Spain), SWED_WHOL (Sweden), SWIT_WHOL (Switzerland), TURK_WHOL (Turkey), UNKI_WHOL (United Kingdom)
   * - **EEU**
     - 17
     - ALBA_WHOL (Albania), BOHE_WHOL (Bosnia and Herzegovina), BULG_WHOL (Bulgaria), CROA_WHOL (Croatia), CZRE_WHOL (Czech Republic), ESTO_WHOL (Estonia), HUNG_WHOL (Hungary), KOSO_WHOL (Kosovo), LATV_WHOL (Latvia), LITH_WHOL (Lithuania), MACE_WHOL (TFYR Macedonia), MONT_WHOL (Montenegro), POLA_WHOL (Poland), ROMA_WHOL (Romania), SERB_WHOL (Serbia), SKRE_WHOL (Slovakia), SLOV_WHOL (Slovenia)
   * - **FSU**
     - 13
     - ARME_WHOL (Armenia), AZER_WHOL (Azerbaijan), BELA_WHOL (Belarus), GEOR_WHOL (Georgia), KAZA_WHOL (Kazakhstan), KYRG_WHOL (Kyrgyzstan), MOLD_WHOL (Republic of Moldova), RUSS_ASIA (Russian Federation, 2 regions), RUSS_EURO (Russian Federation, 2 regions), TAJI_WHOL (Tajikistan), TKME_WHOL (Turkmenistan), UKRA_WHOL (Ukraine), UZBE_WHOL (Uzbekistan)
   * - **MEA**
     - 7
     - EAFR_WHOL (12 countries), EGYP_WHOL (Egypt), IRAN_WHOL (Iran), ISRA_WHOL (Israel), MIDE_WHOL (11 countries), NAFR_WHOL (5 countries), SAAR_WHOL (Saudi Arabia)
   * - **AFR**
     - 7
     - EAFR_WHOL (12 countries), KENY_WHOL (Kenya), NIGE_WHOL (Nigeria), RSAF_WHOL (9 countries), SAFR_WHOL (South Africa), TANZ_WHOL (United Republic of Tanzania), WAFR_WHOL (22 countries)
   * - **SAS**
     - 33
     - AFGH_WHOL (Afghanistan), BANG_DHAK (Bangladesh, 2 regions), BANG_REST (Bangladesh, 2 regions), BHUT_WHOL (Bhutan), INDI_ANPR (India, 23 regions), INDI_ASSA (India, 23 regions), INDI_BENG (India, 23 regions), INDI_BIHA (India, 23 regions), INDI_CHHA (India, 23 regions), INDI_DELH (India, 23 regions), INDI_EHIM (India, 23 regions), INDI_GOA (India, 23 regions), INDI_GUJA (India, 23 regions), INDI_HARY (India, 23 regions), INDI_HIPR (India, 23 regions), INDI_JHAR (India, 23 regions), INDI_KARN (India, 23 regions), INDI_KERA (India, 23 regions), INDI_MAHA (India, 23 regions), INDI_MAPR (India, 23 regions), INDI_ORIS (India, 23 regions), INDI_PUNJ (India, 23 regions), INDI_RAJA (India, 23 regions), INDI_TAMI (India, 23 regions), INDI_UTAN (India, 23 regions), INDI_UTPR (India, 23 regions), INDI_WHIM (India, 23 regions), NEPA_WHOL (Nepal), PAKI_KARA (Pakistan, 4 regions), PAKI_NMWP (Pakistan, 4 regions), PAKI_PUNJ (Pakistan, 4 regions), PAKI_SIND (Pakistan, 4 regions), SRIL_WHOL (Sri Lanka)
   * - **CHN**
     - 32
     - CHIN_ANHU (China, 32 regions), CHIN_BEIJ (China, 32 regions), CHIN_CHON (China, 32 regions), CHIN_FUJI (China, 32 regions), CHIN_GANS (China, 32 regions), CHIN_GUAD (China, 32 regions), CHIN_GUAX (China, 32 regions), CHIN_GUIZ (China, 32 regions), CHIN_HAIN (China, 32 regions), CHIN_HEBE (China, 32 regions), CHIN_HEIL (China, 32 regions), CHIN_HENA (China, 32 regions), CHIN_HONG (China, 32 regions), CHIN_HUBE (China, 32 regions), CHIN_HUNA (China, 32 regions), CHIN_JILI (China, 32 regions), CHIN_JINU (China, 32 regions), CHIN_JINX (China, 32 regions), CHIN_LIAO (China, 32 regions), CHIN_NEMO (China, 32 regions), CHIN_NINX (China, 32 regions), CHIN_QING (China, 32 regions), CHIN_SHAA (China, 32 regions), CHIN_SHAN (China, 32 regions), CHIN_SHND (China, 32 regions), CHIN_SHNX (China, 32 regions), CHIN_SICH (China, 32 regions), CHIN_TIAN (China, 32 regions), CHIN_TIBE (China, 32 regions), CHIN_XING (China, 32 regions), CHIN_YUNN (China, 32 regions), CHIN_ZHEJ (China, 32 regions)
   * - **RCPA**
     - 6
     - CAMB_WHOL (Cambodia), KORN_WHOL (North Korea), LAOS_WHOL (Laos), MONG_WHOL (Mongolia), VIET_NORT (Vietnam, 2 regions), VIET_SOUT (Vietnam, 2 regions)
   * - **PAS**
     - 23
     - BRUN_WHOL (Brunei Darussalam), INDO_JAKA (Indonesia, 4 regions), INDO_JAVA (Indonesia, 4 regions), INDO_REST (Indonesia, 4 regions), INDO_SUMA (Indonesia, 4 regions), KORS_NORT (Republic of Korea, 4 regions), KORS_PUSA (Republic of Korea, 4 regions), KORS_SEOI (Republic of Korea, 4 regions), KORS_SOUT (Republic of Korea, 4 regions), MALA_KUAL (Malaysia, 3 regions), MALA_PENM (Malaysia, 3 regions), MALA_SASA (Malaysia, 3 regions), MYAN_WHOL (Myanmar), PHIL_BVMI (Philippines, 3 regions), PHIL_LUZO (Philippines, 3 regions), PHIL_MANI (Philippines, 3 regions), SING_WHOL (Singapore), TAIW_WHOL (Taiwan), THAI_BANG (Thailand, 5 regions), THAI_CVAL (Thailand, 5 regions), THAI_NEPL (Thailand, 5 regions), THAI_NHIG (Thailand, 5 regions), THAI_SPEN (Thailand, 5 regions)
   * - **PAO**
     - 8
     - AUTR_WHOL (Australia), JAPA_CHSH (Japan, 6 regions), JAPA_CHUB (Japan, 6 regions), JAPA_HOTO (Japan, 6 regions), JAPA_KANT (Japan, 6 regions), JAPA_KINK (Japan, 6 regions), JAPA_KYOK (Japan, 6 regions), NZEL_WHOL (New Zealand)

.. note:: Two GAINS regions span more than one |name| region.
   ``CARB_WHOL`` covers the Caribbean, of which Puerto Rico and the
   United States Virgin Islands belong to NAM and the remainder to LAM.
   ``EAFR_WHOL`` covers East Africa, of which Sudan and South Sudan belong
   to MEA and the remainder to AFR.

