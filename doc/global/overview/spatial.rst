.. _spatial:

Regions
*******

The combined MESSAGE-GLOBIOM framework has global coverage and divides the world into 11 regions which are also the native regions of the MESSAGE model (see :numref:`fig-reg` and :numref:`tab-reg` below). GLOBIOM natively operates at the level of 30 regions which in the linkage to MESSAGE are aggregated to the 11 regions as listed in :numref:`tab-globiomreg`.

.. _fig-reg:
.. figure:: /_static/MESSAGE_regions.png
   :width: 800px

   Map of 11 MESSAGE-GLOBIOM regions including their aggregation to the four regions used in the Representative Concentration Pathways (RCPs).

The country definitions of the 11 MESSAGE regions are described in the table below (:numref:`tab-reg`). In some scenarios, the MESSAGE region of FSU (Former Soviet Union) is disaggregated into four sub-regions resulting in a 14-region MESSAGE model.

.. _tab-reg:
.. list-table:: Listing of 11 regions used in MESSAGE-GLOBIOM, including their country definitions.
   :widths: 13 18 69
   :header-rows: 1

   * - MESSAGE regions
     - Definition
     - List of countries
   * - **NAM**
     - North America
     - Canada, Guam, Puerto Rico, United States of America, Virgin Islands
   * - **WEU**
     - Western Europe
     - Andorra, Austria, Azores, Belgium, Canary Islands, Channel Islands, Cyprus, Denmark, Faeroe Islands, Finland, France, Germany, Gibraltar, Greece, Greenland, Iceland, Ireland, Isle of Man, Italy, Liechtenstein, Luxembourg, Madeira, Malta, Monaco, Netherlands, Norway, Portugal, Spain, Sweden, Switzerland, Turkey, United Kingdom
   * - **PAO**
     - Pacific OECD
     - Australia, Japan, New Zealand
   * - **EEU**
     - Central and Eastern Europe
     - Albania, Bosnia and Herzegovina, Bulgaria, Croatia, Czech Republic, The former Yugoslav Rep. of Macedonia, Hungary, Poland, Romania, Slovak Republic, Slovenia, Yugoslavia, Estonia, Latvia, Lithuania
   * - **FSU**
     - Former Soviet Union
     - Armenia, Azerbaijan, Belarus, Georgia, Kazakhstan, Kyrgyzstan, Republic of Moldova, Russian Federation, Tajikistan, Turkmenistan, Ukraine, Uzbekistan
   * - **CPA**
     - Centrally Planned Asia and China
     - Cambodia, China (incl. Hong Kong), Korea (DPR), Laos (PDR), Mongolia, Viet Nam
   * - **SAS**
     - South Asia
     - Afghanistan, Bangladesh, Bhutan, India, Maldives, Nepal, Pakistan, Sri Lanka
   * - **PAS**
     - Other Pacific Asia
     - American Samoa, Brunei Darussalam, Fiji, French Polynesia, Gilbert-Kiribati, Indonesia, Malaysia, Myanmar, New Caledonia, Papua, New Guinea, Philippines, Republic of Korea, Singapore, Solomon Islands, Taiwan (China), Thailand, Tonga, Vanuatu, Western Samoa
   * - **MEA**
     - Middle East and North Africa
     - Algeria, Bahrain, Egypt (Arab Republic), Iraq, Iran (Islamic Republic), Israel, Jordan, Kuwait, Lebanon, Libya/SPLAJ, Morocco, Oman, Qatar, Saudi Arabia, Sudan, Syria (Arab Republic), Tunisia, United Arab Emirates, Yemen
   * - **LAM**
     - Latin America and the Caribbean
     - Antigua and Barbuda, Argentina, Bahamas, Barbados, Belize, Bermuda, Bolivia, Brazil, Chile, Colombia, Costa Rica, Cuba, Dominica, Dominican Republic, Ecuador, El Salvador, French Guyana, Grenada, Guadeloupe, Guatemala, Guyana, Haiti, Honduras, Jamaica, Martinique, Mexico, Netherlands Antilles, Nicaragua, Panama, Paraguay, Peru, Saint Kitts and Nevis, Santa Lucia, Saint Vincent and the Grenadines, Suriname, Trinidad and Tobago, Uruguay, Venezuela
   * - **AFR**
     - Sub-Saharan Africa
     - Angola, Benin, Botswana, British Indian Ocean Territory, Burkina Faso, Burundi, Cameroon, Cape Verde, Central African Republic, Chad, Comoros, Cote d'Ivoire, Congo, Democratic Republic of Congo, Djibouti, Equatorial Guinea, Eritrea, Ethiopia, Gabon, Gambia, Ghana, Guinea, Guinea-Bissau, Kenya, Lesotho, Liberia, Madagascar, Malawi, Mali, Mauritania, Mauritius, Mozambique, Namibia, Niger, Nigeria, Reunion, Rwanda, Sao Tome and Principe, Senegal, Seychelles, Sierra Leone, Somalia, South Africa, Saint Helena, Swaziland, Tanzania, Togo, Uganda, Zambia, Zimbabwe   |

In addition to the 11 geographical regions, in MESSAGE there is a global trade region where market clearing of global energy markets is happening and international shipping bunker fuel demand, uranium resource extraction and the nuclear fuel cycle are represented.

.. _tab-globiomreg:
.. list-table:: Listing of 30 regions used in GLOBIOM, including their country definitions and the mapping to the 11 regions of the combined MESSAGE-GLOBIOM model.
   :widths: 13 17 70
   :header-rows: 1

   * - MESSAGE regions
     - GLOBIOM regions
     - List of countries
   * - **NAM**
     - Canada
     - Canada
   * -
     - USA
     - United States of America
   * - **WEU**
     - EU_MidWest
     - Austria, Belgium, Germany, France, Luxembourg, Netherlands
   * -
     - EU_North
     - Denmark, Finland, Ireland, Sweden, United Kingdom
   * -
     - EU_South
     - Cyprus, Greece, Italy, Malta, Portugal, Spain
   * -
     - ROWE
     - Gibraltar, Iceland, Norway, Switzerland
   * -
     - Turkey
     - Turkey
   * - **PAO**
     - ANZ
     - Australia, New Zealand
   * -
     - Japan
     - Japan
   * -
     - Pacific_Islands
     - Fiji Islands, Kiribati, Papua New Guinea, Samoa, Solomon Islands, Tonga, Vanuatu
   * - **EEU**
     - EU_Baltic
     - Estonia, Latvia, Lithuania
   * -
     - EU_CentEast
     - Bulgaria, Czech Republic, Hungary, Poland, Romania, Slovakia, Slovenia
   * -
     - RCEU
     - Albania, Bosnia and Herzegovina, Croatia, Macedonia, Serbia-Montenegro
   * - **FSU**
     - Former_USSR
     - Armenia, Azerbaijan, Belarus, Georgia, Kazakhstan, Kyrgyzstan, Moldova, Russian Federation, Tajikistan, Turkmenistan, Ukraine, Uzbekistan
   * - **CPA**
     - China
     - China
   * -
     - RSEA_PAC
     - Cambodia, Korea DPR, Laos, Mongolia, Viet Nam
   * - **SAS**
     - India
     - India
   * -
     - RSAS
     - Afghanistan, Bangladesh, Bhutan, Maldives, Nepal, Pakistan, Sri Lanka
   * - **PAS**
     - South_Korea
     - South Korea
   * -
     - RSEA_OPA
     - Brunei Daressalaam, Indonesia, Singapore, Malaysia, Myanmar, Philippines, Thailand
   * - **MEA**
     - MidEastNAfr
     - Algeria, Bahrain, Egypt, Iran, Iraq, Israel, Jordan, Kuwait, Lebanon, Libya, Morocco, Oman, Qatar, Saudi Arabia, Syria, Tunisia, United Arab Emirates, Yemen
   * - **LAM**
     - Brazil
     - Brazil
   * -
     - Mexico
     - Mexico
   * -
     - RCAM
     - Bahamas, Barbados, Belize, Bermuda, Costa Rica, Cuba, Dominica, Dominican Republic, El Salvador, Grenada, Guatemala, Haiti, Honduras, Jamaica, Nicaragua, Netherland Antilles, Panama, St Lucia, St Vincent, Trinidad and Tobago
   * -
     - RSAM
     - Argentina, Bolivia, Chile, Colombia, Ecuador, Guyana, Paraguay, Peru, Suriname, Uruguay, Venezuela
   * - **AFR**
     - Congo_Basin
     - Cameroon, Central African Republic, Congo Republic, Democratic Republic of Congo, Equatorial, Guinea, Gabon
   * -
     - EasternAf
     - Burundi, Ethiopia, Kenya, Rwanda, Tanzania, Uganda
   * -
     - SouthAf
     - South Africa
   * -
     - RoSAfr
     - Angola, Botswana, Comoros, Lesotho, Madagascar, Malawi, Mauritius, Mozambique, Namibia, Swaziland, Zambia, Zimbabwe
   * -
     - WestCentAfr
     - Benin, Burkina Faso, Cape Verde, Chad, Cote d'Ivoire, Djibouti, Eritrea, Gambia, Ghana, Guinea, Guinea Bissau, Liberia, Mali, Mauritania, Niger, Nigeria, Senegal, Sierra Leone, Somalia, Sudan, Togo
