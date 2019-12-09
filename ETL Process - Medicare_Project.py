#!/usr/bin/env python
# coding: utf-8

####################Create database tables#################



import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship

# Pass the connection string to a variable, conn_url
conn_url = 'postgresql://postgres:4rabf037@f19server.apan5310.com:50201/Medicare_project_final'

import psycopg2
# Create an engine that connects to PostgreSQL server
engine = create_engine(conn_url)

# Establish a connection
connection = engine.connect()


# Pass the SQL statements that create all tables
stmt = """
    CREATE TABLE provider_names(

        name_id int,

        provider_lastname_organization_name varchar(100),

        first_name varchar(50),

        middle_initial varchar(2),

        credentials varchar(20),

        gender varchar(1),

        PRIMARY KEY (name_id)

        );
    CREATE TABLE providers_misc (

        providers_misc_id int,

        entity_code varchar(1),

        average_age_beneficiaries int,

        average_hcc_risk_score_ben numeric(6,4),

        PRIMARY KEY (providers_misc_id)

        );

    CREATE TABLE provider_addresses (

        address_id int,

        street_address varchar,

        city varchar,

        zip_code varchar,

        state varchar,

        country varchar,

        PRIMARY KEY (address_id)

        );

    CREATE TABLE provider_types( provider_type_id int,

        provider_type varchar(75) NOT NULL,

        PRIMARY KEY (provider_type_id)

        );

    CREATE TABLE medicare_participants(

        medicare_participant_id int,

        med_participation varchar(1) NOT NULL,

        number_medicareBen int,

        PRIMARY KEY (medicare_participant_id)

        );

    CREATE TABLE providers(

        npi int,

        name_id int,

        provider_type_id int,

        address_id int,

        medicare_participant_id int,

        providers_misc_id int,

        PRIMARY KEY (npi),

        FOREIGN KEY (name_id) REFERENCES provider_names(name_id),

        FOREIGN KEY (provider_type_id) REFERENCES provider_types(provider_type_id),

        FOREIGN KEY (address_id) REFERENCES provider_addresses(address_id),

        FOREIGN KEY (medicare_participant_id) REFERENCES medicare_participants(medicare_participant_id),

        FOREIGN KEY (providers_misc_id) REFERENCES providers_misc(providers_misc_id)

            ON DELETE CASCADE

            ON UPDATE CASCADE

            ); 
    CREATE TABLE medicare_charges (

        charges_id int,

        npi int,

        submitted_charges_amount int,

        medicare_allowed_amount int,

        medicare_payment_amount int,

        PRIMARY KEY (charges_id),

        FOREIGN KEY (npi) REFERENCES providers (npi)

            ON DELETE CASCADE

            ON UPDATE CASCADE

            );

    CREATE TABLE gender_beneficiaries(

        npi int,

        gender varchar(1),

        count int,

        PRIMARY KEY(npi,gender),

        FOREIGN KEY (npi) REFERENCES providers (npi)

            ON DELETE CASCADE

            ON UPDATE CASCADE

            );

    CREATE TABLE race_beneficiaries (

        npi int,

        race varchar(40),

        count int,

        PRIMARY KEY (npi, race), 
        
        FOREIGN KEY (npi) REFERENCES providers (npi)
        
        ON DELETE CASCADE            
            
        ON UPDATE CASCADE,

        CHECK(race IN('non-hispanic_white','black_or_african_american','asian_pacific_islander','hispanic','american_indianalaska_native','other'))     
        );

    CREATE TABLE entitlement_beneficiaries (

        npi int,

        entitlement_type varchar,

        count int,

        PRIMARY KEY (npi, entitlement_type),

        FOREIGN KEY (npi) REFERENCES providers (npi)
        
            ON DELETE CASCADE

            ON UPDATE CASCADE,
            
        CHECK (entitlement_type IN ('medicare_only' , 'medicare_medicaid'))
        
            );

    CREATE TABLE chronic_illness (

        npi int,

        chronic_illness varchar(50),

        percent int,

        PRIMARY KEY (npi, chronic_illness),

        FOREIGN KEY (npi) REFERENCES providers (npi)
            
            ON DELETE CASCADE 
            
            ON UPDATE CASCADE,

        CHECK (chronic_illness IN ('Atrial Fibrillation' ,'Alzheimer’s Disease or Dementia', 'Asthma', 'Cancer', 'Heart Failure', 'Chronic Kidney Disease', 'Chronic Obstructive Pulmonary Disease', 'Depression', 'Diabetes', 'Hyperlipidemia', 'Hypertension', 'Ischemic Heart Disease', 'Osteoporosis', 'Rheumatoid Arthritis / Osteoarthritis', 'Schizophrenia / Other Psychotic Disorders', 'Stroke'))

            );

    CREATE TABLE age_range_beneficiaries (

        npi int,

        age_range varchar(20),

        count int,

        PRIMARY KEY (npi, age_range),

        FOREIGN KEY (npi) REFERENCES providers (npi)
            
            ON DELETE CASCADE 
            
            ON UPDATE CASCADE        
        ,

        CHECK (age_range IN('Age Less 65','Age 65 to 74','Age 75 to 84','Age Greater 84'))

            );

    CREATE TABLE medicare_drug_payments (

        drug_payments_id int,

        npi int,

        number_medicare_beneficiaries_with_drug int,

        drug_submitted_charges numeric(12,2),

        drug_medicare_allowed numeric(12,2),

        drug_medicare_payment numeric(12,2),

        PRIMARY KEY (drug_payments_id),
           
        FOREIGN KEY (npi) REFERENCES providers (npi)
        
            ON DELETE CASCADE

            ON UPDATE CASCADE

            );

    CREATE TABLE medical_payment ( 
        
        medical_payment_id int,

        npi int,

        submitted_charges numeric (12,2),

        medicare_allowed numeric (12,2),

        medicare_payment numeric (12,2),

        PRIMARY KEY (medical_payment_id),

        FOREIGN KEY (npi) REFERENCES providers (npi)

            ON DELETE CASCADE

            ON UPDATE CASCADE

        );

    CREATE TABLE drug_services (

        drug_services_id int,

        npi int,

        drug_suppress_indicator varchar(1),

        number_of_hcpcs_associated_with_drug_services int,

        number_of_drug_services int,

        PRIMARY KEY (drug_services_id),

        FOREIGN KEY (npi) REFERENCES providers (npi)

            ON DELETE CASCADE

            ON UPDATE CASCADE

            );

    CREATE TABLE medical_services (

        medical_services_id int,

        npi int,

        medical_suppress_indicator varchar(1),

        number_of_hcpcs_medical_services int, number_medical_services int,

        Number_of_medicare_beneficiaries_with_medical_services int,

        PRIMARY KEY (medical_services_id),

        FOREIGN KEY (npi) references providers(npi)

            ON DELETE CASCADE

            ON UPDATE CASCADE

            );
"""


# Execute the statement to create tables
connection.execute(stmt)


#########################Extract, Transform and Load (ETL)##########################



df = pd.read_csv('medicare-physician-and-other-supplier-national-provider-identifier-npi-aggregate-report-calendar-year-2012.csv',low_memory=False)



# Since the dataset is large and the processing time into team server is at least 10 hours,we decided to select the top 50000
df = df[:50000]



# Change column name: replace special character with '_' and change to lower case
df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('/', '').str.replace('%', '')



df.info()


#################provider_names#################


# Subset certain columns to get provider_names table
provider_names = df[['nppes_provider_last_name__organization_name', 'nppes_provider_first_name','nppes_provider_middle_initial','nppes_provider_middle_initial','nppes_provider_gender']]


# Check NA value in provider last name, there is no NA calue in last name, some NA in first name.
provider_names.nppes_provider_last_name__organization_name.isna().sum()


# Drop duplicate 
provider_names=provider_names.drop_duplicates()


# Although there are duplicated names, since the npi is unique, we can ignore the duplicate, and take them as unique providers.
# Add name_id index
provider_names.insert(0, 'name_id', range(1, 1 + len(provider_names)))


#################provider_names#################


# Rename the columns to align with the schema we set before
provider_names.columns = ['name_id','provider_lastname_organization_name','first_name','middle_initial','credentials','gender']


# Populate the database
provider_names[['name_id','provider_lastname_organization_name','first_name','middle_initial','credentials','gender']].     drop_duplicates().to_sql(name='provider_names', con=engine, if_exists='append', index=False)


# Map drug_name_id
name_id_list = [provider_names.name_id[provider_names.provider_lastname_organization_name == i].values[0] for i in df.nppes_provider_last_name__organization_name]


# Add name_id to the main dataframe
df.insert(1, 'name_id', name_id_list)


#################providers_misc#################


# Subset certain columns to get provider_misc table
providers_misc = df[['nppes_entity_code','average_age_of_beneficiaries','average_hcc_risk_score_of_beneficiaries']]


# Check number of NA value in entity code
providers_misc.nppes_entity_code.isna().sum() # 0


# Drop duplicates
providers_misc = providers_misc.drop_duplicates()


# Add providers_misc_id index
providers_misc.insert(0, 'providers_misc_id', range(1, 1 + len(providers_misc)))


# Rename the columns to align with the schema we set before
providers_misc.columns = ['providers_misc_id','entity_code','average_age_beneficiaries','average_hcc_risk_score_ben']


# Populate the database
providers_misc[['providers_misc_id','entity_code','average_age_beneficiaries','average_hcc_risk_score_ben']].     drop_duplicates().to_sql(name='providers_misc', con=engine, if_exists='append', index=False)


# Map providers_misc_id
providers_misc_id_list = [providers_misc.providers_misc_id[providers_misc.average_hcc_risk_score_ben == i].values[0] for i in df.average_hcc_risk_score_of_beneficiaries]

# Add name_id to the main dataframe
df.insert(5, 'providers_misc_id', providers_misc_id_list)


#################provider_addresses#################


# Check number of NA value in address_1
df.nppes_provider_street_address_1.isna().sum() # 0



# Merge address_1 and address_2, which is the apartment number
df['nppes_provider_street_address_1'] = df.nppes_provider_street_address_1.astype(str).str.cat(df.nppes_provider_street_address_1.astype(str), sep='q')



df['nppes_provider_street_address_1']



# Subset certain columns to get provider_misc table
provider_addresses = df[['nppes_provider_street_address_1','nppes_provider_city','nppes_provider_zip_code','nppes_provider_state','nppes_provider_country']]



# Drop duplicates
provider_addresses = provider_addresses.drop_duplicates()



# Add address_id index
provider_addresses.insert(0, 'address_id', range(1, 1 + len(provider_addresses)))



# Rename the columns to align with the schema we set before
provider_addresses.columns = ['address_id','street_address','city','zip_code','state','country']



#################provider_addresses#################


# Populate the database
provider_addresses[['address_id','street_address','city','zip_code','state','country']].     drop_duplicates().to_sql(name='provider_addresses', con=engine, if_exists='append', index=False)


# Map providers_misc_id
provider_addresses_id_list = [provider_addresses.address_id[provider_addresses.street_address == i].values[0] for i in df.nppes_provider_street_address_1]


# Add name_id to the main dataframe
df.insert(8, 'address_id', provider_addresses_id_list)


#################provider_types#################


# Check number of NA value in provider type
df.provider_type.isna().sum() # 0


# Subset certain columns to get provider_misc table
provider_types = df[['provider_type']]


# Drop duplicates
provider_types = provider_types.drop_duplicates()

# Add index
provider_types.insert(0, 'provider_type_id', range(1, 1 + len(provider_types)))


# In[42]:


# Populate the database
provider_types[['provider_type_id','provider_type']].     drop_duplicates().to_sql(name='provider_types', con=engine, if_exists='append', index=False)


# Map providers_misc_id
provider_types_id_list = [provider_types.provider_type_id[provider_types.provider_type == i].values[0] for i in df.provider_type]

# Add name_id to the main dataframe
df.insert(10, 'provider_type_id', provider_types_id_list)


#################medicare_participants#################


# Check number of NA value in medicare_participation_indicator
df.medicare_participation_indicator.isna().sum() # 0


# Subset certain columns to get medicare_participants table
medicare_participants = df[['medicare_participation_indicator','number_of_unique_beneficiaries']]


# Drop duplicates
medicare_participants = medicare_participants.drop_duplicates()

# Add index
medicare_participants.insert(0, 'medicare_participant_id', range(1, 1 + len(medicare_participants)))


#################medicare_participants#################


# Rename the columns to align with the schema we set before
medicare_participants.columns = ['medicare_participant_id','med_participation','number_medicareben']


# Populate the database
medicare_participants[['medicare_participant_id','med_participation','number_medicareben']].     drop_duplicates().to_sql(name='medicare_participants', con=engine, if_exists='append', index=False)


# Map providers_misc_id
medicare_participants_id_list = [medicare_participants.medicare_participant_id[medicare_participants.number_medicareben == i].values[0] for i in df.number_of_unique_beneficiaries]

# Add name_id to the main dataframe
df.insert(16, 'medicare_participant_id', medicare_participants_id_list)


#################providers#################



# Subset certain columns to get providers table
providers = df[['npi','name_id','provider_type_id','address_id','medicare_participant_id','providers_misc_id']]



#################providers#################



# Populate the database
providers[['npi','name_id','provider_type_id','address_id','medicare_participant_id','providers_misc_id']].     drop_duplicates().to_sql(name='providers', con=engine, if_exists='append', index=False)



# Add delete and update cascade
providers = relationship("providers", cascade="all,delete", backref="provider_names")


#################medicare_charges#################



# Check number of NA value in total_submitted_charges
df.total_submitted_charges.isna().sum() # 0



# Subset certain columns to get medicare_charges table
medicare_charges = df[['npi','total_submitted_charges','total_medicare_allowed_amount','total_medicare_payment_amount']]



# Drop duplicates
medicare_charges = medicare_charges.drop_duplicates()

# Add index
medicare_charges.insert(0, 'charges_id', range(1, 1 + len(medicare_charges)))



# Rename the columns to align with the schema we set before
medicare_charges.columns = ['charges_id','npi','submitted_charges_amount','medicare_allowed_amount','medicare_payment_amount']



#################medicare_charges#################



# Populate the database
medicare_charges[['charges_id','npi','submitted_charges_amount','medicare_allowed_amount','medicare_payment_amount']].     drop_duplicates().to_sql(name='medicare_charges', con=engine, if_exists='append', index=False)



# Add delete and update cascade
medicare_charges = relationship("medicare_charges", cascade="all,delete", backref="providers")



# Map providers_misc_id
medicare_charges_id_list = [medicare_charges.charges_id[medicare_charges.npi == i].values[0] for i in df.npi]

# Add name_id to the main dataframe
df.insert(19, 'charges_id', medicare_charges_id_list)


#################gender_beneficiaries#################



# Subset certain columns to get gender_beneficiaries table
gender_beneficiaries = df[['npi','number_of_female_beneficiaries','number_of_male_beneficiaries']]



# Subset certain columns to get female table
female = gender_beneficiaries[['npi','number_of_female_beneficiaries']]



# Subset certain columns to get male table
male = gender_beneficiaries[['npi','number_of_male_beneficiaries']]



# Add gender column, label female as f
female['gender'] = 'f' 



# Add gender column, label male as m
male['gender'] = 'm' 


# Rename female table to be able to merge with male table
female.columns = ['npi','count','gender']



# Rename male table to be able to merge with female table
male.columns = ['npi','count','gender']



# Merge female and male table
gender_beneficiaries = pd.concat([female,male])



# Get the final table which matched with the designed schema
gender_beneficiaries



# Populate the database
gender_beneficiaries[['npi','gender','count']].     drop_duplicates().to_sql(name='gender_beneficiaries', con=engine, if_exists='append', index=False)



# Add delete and update cascade
gender_beneficiaries = relationship("gender_beneficiaries", cascade="all,delete", backref="providers")


#################race_beneficiaries#################



# Subset certain columns to get race_black table
race_black = df[['npi','number_of_black_or_african_american_beneficiaries']]



# Add a race column to label certain race 
race_black['race'] = 'black_or_african_american' 



# Filter out null value rows
race_black = race_black[race_black.number_of_black_or_african_american_beneficiaries.notnull()]



# Rename columns to be able to merge with other race
race_black.columns = ['npi','count','race']



# Filter out 0 value rows
race_black = race_black[race_black['count']!=0]



# Subset certain columns to get race_white table
race_white = df[['npi','number_of_non-hispanic_white_beneficiaries']]



# Add a race column to label certain race 
race_white['race'] = 'non-hispanic_white' 



# Rename columns to be able to merge with other race
race_white.columns = ['npi','number_of_non_hispanic_white_beneficiaries','race']



# Filter out null value rows
race_white = race_white[race_white.number_of_non_hispanic_white_beneficiaries.notnull()]



# Rename columns to be able to merge with other race
race_white.columns = ['npi','count','race']



# Filter out 0 value rows
race_white = race_white[race_white['count']!=0]


# In[93]:


# Same process as the white and black above
race_asian = df[['npi','number_of_asian_pacific_islander_beneficiaries']]
race_asian['race'] = 'asian_pacific_islander'
race_asian = race_asian[race_asian.number_of_asian_pacific_islander_beneficiaries.notnull()]
race_asian.columns = ['npi','count','race']
race_asian = race_asian[race_asian['count']!=0]



race_his = df[['npi','number_of_hispanic_beneficiaries']]
race_his['race'] = 'hispanic'
race_his = race_his[race_his.number_of_hispanic_beneficiaries.notnull()]
race_his.columns = ['npi','count','race']
race_his = race_his[race_his['count']!=0]




race_ind = df[['npi','number_of_american_indianalaska_native_beneficiaries']]
race_ind['race'] = 'american_indianalaska_native'
race_ind = race_ind[race_ind.number_of_american_indianalaska_native_beneficiaries.notnull()]
race_ind.columns = ['npi','count','race']
race_ind = race_ind[race_ind['count']!=0]




race_other = df[['npi','number_of_beneficiaries_with_race_not_elsewhere_classified']]
race_other['race'] = 'other'
race_other = race_other[race_other.number_of_beneficiaries_with_race_not_elsewhere_classified.notnull()]
race_other.columns = ['npi','count','race']
race_other = race_other[race_other['count']!=0]




# Conmibe race tables into one
race_df = pd.concat([race_black,race_white,race_asian,race_his,race_ind,race_other])



# Take a look at the sorted race_df
race_df.sort_index(axis=0)



# Populate table into the database
race_df[['npi','race','count']].     drop_duplicates().to_sql(name='race_beneficiaries', con=engine, if_exists='append', index=False)



# Add delete and update cascade
race_beneficiaries = relationship("race_beneficiaries", cascade="all,delete", backref="providers")


#################entitlement_beneficiaries#################


# Subset certain columns to get medicare table
medicare = df[['npi','number_of_beneficiaries_with_medicare_only_entitlement']] 



# Add entitlement_type column and rename to fit the schema, filter out NA and 0
medicare['entitlement_type'] = 'medicare_only'
medicare = medicare[medicare.number_of_beneficiaries_with_medicare_only_entitlement.notnull()]
medicare = medicare[medicare['number_of_beneficiaries_with_medicare_only_entitlement']!=0]
medicare.columns = ['npi','count','entitlement_type']



# Same process as medicare
medicare_medicaid = df[['npi','number_of_beneficiaries_with_medicare_&_medicaid_entitlement']] 
medicare_medicaid['entitlement_type'] = 'medicare_medicaid'
medicare_medicaid.columns = ['npi','count','entitlement_type']
medicare_medicaid
medicare_medicaid = medicare_medicaid[medicare_medicaid['count']!=0]
medicare_medicaid = medicare_medicaid.dropna()




# Merge two tables into one
entitlement_beneficiaries  = pd.concat([medicare,medicare_medicaid])




# Check the sorted table
entitlement_beneficiaries.sort_index(axis=0)



# Populate the database
entitlement_beneficiaries[['npi','entitlement_type','count']].     drop_duplicates().to_sql(name='entitlement_beneficiaries', con=engine, if_exists='append', index=False)



# Add delete and update cascade
entitlement_beneficiaries = relationship("entitlement_beneficiaries", cascade="all,delete", backref="providers")


#################chronic_illness################# 



# Subset certain columns to get atrial table
atrial = df[['npi','percent__of_beneficiaries_identified_with_atrial_fibrillation']]



# Add chronic_illness column and rename to fit the schema, filter out the NA and 0
atrial['chronic_illness'] = 'Atrial Fibrillation' 
atrial.columns = ['npi','percent','chronic_illness']
atrial = atrial[atrial.percent.notnull()]
atrial = atrial[atrial['percent']!=0]



# Same process as atrial
Alzheimer = df[['npi','percent__of_beneficiaries_identified_with_alzheimer’s_disease_or_dementia']]



Alzheimer['chronic_illness'] = 'Alzheimer’s Disease or Dementia' 
Alzheimer.columns = ['npi','percent','chronic_illness']
Alzheimer = Alzheimer[Alzheimer.percent.notnull()]
Alzheimer = Alzheimer[Alzheimer['percent']!=0]



Asthma = df[['npi','percent__of_beneficiaries_identified_with_asthma']]
Asthma['chronic_illness'] = 'Asthma' 
Asthma.columns = ['npi','percent','chronic_illness']
Asthma = Asthma[Asthma.percent.notnull()]
Asthma = Asthma[Asthma['percent']!=0]




Cancer = df[['npi','percent__of_beneficiaries_identified_with_cancer']]
Cancer['chronic_illness'] = 'Cancer' 
Cancer.columns = ['npi','percent','chronic_illness']
Cancer = Cancer[Cancer.percent.notnull()]
Cancer = Cancer[Cancer['percent']!=0]




heart = df[['npi','percent__of_beneficiaries_identified_with_heart_failure']]
heart['chronic_illness'] = 'Heart Failure' 
heart.columns = ['npi','percent','chronic_illness']
heart = heart[heart.percent.notnull()]
heart = heart[heart['percent']!=0]




Kidney = df[['npi','percent__of_beneficiaries_identified_with_chronic_kidney_disease']]
Kidney['chronic_illness'] = 'Chronic Kidney Disease' 
Kidney.columns = ['npi','percent','chronic_illness']
Kidney = Kidney[Kidney.percent.notnull()]
Kidney = Kidney[Kidney['percent']!=0]




Pulmonary = df[['npi','percent__of_beneficiaries_identified_with_chronic_obstructive_pulmonary_disease']]
Pulmonary['chronic_illness'] = 'Chronic Obstructive Pulmonary Disease'
Pulmonary.columns = ['npi','percent','chronic_illness']
Pulmonary = Pulmonary[Pulmonary.percent.notnull()]
Pulmonary = Pulmonary[Pulmonary['percent']!=0]




Depression = df[['npi','percent__of_beneficiaries_identified_with_depression']]
Depression['chronic_illness'] = 'Depression'
Depression.columns = ['npi','percent','chronic_illness']
Depression = Depression[Depression.percent.notnull()]
Depression = Depression[Depression['percent']!=0]




Diabetes = df[['npi','percent__of_beneficiaries_identified_with_diabetes']]
Diabetes['chronic_illness'] = 'Diabetes'
Diabetes.columns = ['npi','percent','chronic_illness']
Diabetes = Diabetes[Diabetes.percent.notnull()]
Diabetes = Diabetes[Diabetes['percent']!=0]




Hyperlipidemia = df[['npi','percent__of_beneficiaries_identified_with_hyperlipidemia']]
Hyperlipidemia['chronic_illness'] = 'Hyperlipidemia'
Hyperlipidemia.columns = ['npi','percent','chronic_illness']
Hyperlipidemia = Hyperlipidemia[Hyperlipidemia.percent.notnull()]
Hyperlipidemia = Hyperlipidemia[Hyperlipidemia['percent']!=0]




Hypertension = df[['npi','percent__of_beneficiaries_identified_with_hypertension']]
Hypertension['chronic_illness'] = 'Hypertension'
Hypertension.columns = ['npi','percent','chronic_illness']
Hypertension = Hypertension[Hypertension.percent.notnull()]
Hypertension = Hypertension[Hypertension['percent']!=0]




Ischemic = df[['npi','percent__of_beneficiaries_identified_with_ischemic_heart_disease']]
Ischemic['chronic_illness'] = 'Ischemic Heart Disease'
Ischemic.columns = ['npi','percent','chronic_illness']
Ischemic = Ischemic[Ischemic.percent.notnull()]
Ischemic = Ischemic[Ischemic['percent']!=0]




Osteoporosis = df[['npi','percent__of_beneficiaries_identified_with_osteoporosis']]
Osteoporosis['chronic_illness'] = 'Osteoporosis'
Osteoporosis.columns = ['npi','percent','chronic_illness']
Osteoporosis = Osteoporosis[Osteoporosis.percent.notnull()]
Osteoporosis = Osteoporosis[Osteoporosis['percent']!=0]




Rheumatoid = df[['npi','percent__of_beneficiaries_identified_with_rheumatoid_arthritis__osteoarthritis']]
Rheumatoid['chronic_illness'] = 'Rheumatoid Arthritis / Osteoarthritis'
Rheumatoid.columns = ['npi','percent','chronic_illness']
Rheumatoid = Rheumatoid[Rheumatoid.percent.notnull()]
Rheumatoid = Rheumatoid[Rheumatoid['percent']!=0]




Schizophrenia = df[['npi','percent__of_beneficiaries_identified_with_schizophrenia__other_psychotic_disorders']]
Schizophrenia['chronic_illness'] = 'Schizophrenia / Other Psychotic Disorders'
Schizophrenia.columns = ['npi','percent','chronic_illness']
Schizophrenia = Schizophrenia[Schizophrenia.percent.notnull()]
Schizophrenia = Schizophrenia[Schizophrenia['percent']!=0]




Stroke = df[['npi','percent__of_beneficiaries_identified_with_stroke']]
Stroke['chronic_illness'] = 'Stroke'
Stroke.columns = ['npi','percent','chronic_illness']
Stroke = Stroke[Stroke.percent.notnull()]
Stroke = Stroke[Stroke['percent']!=0]




# Merge all the illness tables into one
chronic_illness = pd.concat([atrial,Alzheimer,Asthma,Cancer,heart,Kidney,Pulmonary,Depression,Diabetes,Hyperlipidemia,Hypertension,Ischemic,Osteoporosis,Rheumatoid,Schizophrenia,Stroke])



# Populate the database
chronic_illness[['npi','chronic_illness','percent']].     drop_duplicates().to_sql(name='chronic_illness', con=engine, if_exists='append', index=False)




# Add delete and update cascade
chronic_illness = relationship("chronic_illness", cascade="all,delete", backref="providers")


#################age_range_beneficiaries#################



# Subset the relevant columns, add age_range and rename to fit the schema, filter out the NA and 0 
less_than_65 = df[['npi','number_of_beneficiaries_age_less_than_65']]
less_than_65['age_range'] = 'Age Less 65'
less_than_65.columns = ['npi','count','age_range']
less_than_65 = less_than_65.dropna()
less_than_65 = less_than_65[less_than_65['count']!=0]




# Same process as less_than_65
age_65to74 = df[['npi','number_of_beneficiaries_age_65_to_74']]
age_65to74['age_range'] = 'Age 65 to 74'
age_65to74.columns = ['npi','count','age_range']
age_65to74 = age_65to74.dropna()
age_65to74 = age_65to74[age_65to74['count']!=0]




Age_75to84 = df[['npi','number_of_beneficiaries_age_75_to_84']]
Age_75to84['age_range'] = 'Age 75 to 84'
Age_75to84.columns = ['npi','count','age_range']
Age_75to84 = Age_75to84.dropna()
Age_75to84 = Age_75to84[Age_75to84['count']!=0]




greater84 = df[['npi','number_of_beneficiaries_age_greater_84']]
greater84['age_range'] = 'Age Greater 84'
greater84.columns = ['npi','count','age_range']
greater84 = greater84.dropna()
greater84 = greater84[greater84['count']!=0]




# Merge all age ranges into one
age_range_beneficiaries = pd.concat([less_than_65,age_65to74,Age_75to84,greater84])




# Populate the database
age_range_beneficiaries[['npi','age_range','count']].     drop_duplicates().to_sql(name='age_range_beneficiaries', con=engine, if_exists='append', index=False)
age_range_beneficiaries = relationship("age_range_beneficiaries", cascade="all,delete", backref="providers")


#################medicare_drug_payments################# 



# Subset relevant columns to get medicare_drug_payments table
medicare_drug_payments = df[['npi','number_of_unique_beneficiaries_with_drug_services','total_drug_submitted_charges','total_drug_medicare_allowed_amount','total_drug_medicare_payment_amount']]




# Drop duplicates
medicare_drug_payments = medicare_drug_payments.drop_duplicates()
medicare_drug_payments = medicare_drug_payments.dropna()
# Add index
medicare_drug_payments.insert(0, 'drug_payments_id', range(1, 1 + len(medicare_drug_payments)))



# Rename to fit the schema
medicare_drug_payments.columns = ['drug_payments_id','npi','number_medicare_beneficiaries_with_drug','drug_submitted_charges','drug_medicare_allowed','drug_medicare_payment']




# Populate the database
medicare_drug_payments[['drug_payments_id','npi','number_medicare_beneficiaries_with_drug','drug_submitted_charges','drug_medicare_allowed','drug_medicare_payment']].     drop_duplicates().to_sql(name='medicare_drug_payments', con=engine, if_exists='append', index=False)




# Add delete and update cascade 
medicare_drug_payments = relationship("medicare_drug_payments", cascade="all,delete", backref="providers")



# Map providers_misc_id
medicare_drug_payments_id_list = [medicare_drug_payments.drug_payments_id[medicare_drug_payments.drug_submitted_charges == i].values[0] for i in df.total_drug_submitted_charges]

# Add name_id to the main dataframe
df.insert(24, 'drug_payments_id', medicare_drug_payments_id_list)


#################medical_payment################# 



# Subset relevant columns to get medical_payment table
medical_payment = df[['npi','total_medical_submitted_charges','total_medical_medicare_allowed_amount','total_medical_medicare_payment_amount']]



# Drop duplicates and NA
medical_payment = medical_payment.drop_duplicates()
medical_payment = medical_payment.dropna()

# Add index
medical_payment.insert(0, 'medical_payment_id', range(1, 1 + len(medical_payment)))



# Rename to fit the schema
medical_payment.columns = ['medical_payment_id','npi','submitted_charges','medicare_allowed','medicare_payment']



# Populate the database
medical_payment[['medical_payment_id','npi','submitted_charges','medicare_allowed','medicare_payment']].     drop_duplicates().to_sql(name='medical_payment', con=engine, if_exists='append', index=False)



# Add delete and update cascade
medical_payment = relationship("medical_payment", cascade="all,delete", backref="providers")



# Map providers_misc_id
medical_payment_id_list = [medical_payment.medical_payment_id[medical_payment.submitted_charges == i].values[0] for i in df.total_medical_submitted_charges]

# Add name_id to the main dataframe
df.insert(30, 'medical_payment_id', medical_payment_id_list)


#################drug_services################# 



# Subset relevant columns to get medical_payment table
drug_services = df[['npi','drug_suppress_indicator','number_of_hcpcs_associated_with_drug_services','number_of_drug_services']]



# Drop duplicates
drug_services = drug_services.drop_duplicates()

# Add index
drug_services.insert(0, 'drug_services_id', range(1, 1 + len(drug_services)))



# Change * with Y, meaning this drug has been suppressed, NA with N, meaning no suppression existists.
drug_services['drug_suppress_indicator'] = drug_services.drug_suppress_indicator.fillna('N')
drug_services.drug_suppress_indicator[drug_services.drug_suppress_indicator == '*'] = 'Y'



# Rename to fit the schema
drug_services.columns = ['drug_services_id','npi','drug_suppress_indicator','number_of_hcpcs_associated_with_drug_services','number_of_drug_services']



# Populate the database
drug_services[['drug_services_id','npi','drug_suppress_indicator','number_of_hcpcs_associated_with_drug_services','number_of_drug_services']].     drop_duplicates().to_sql(name='drug_services', con=engine, if_exists='append', index=False)



# Add delete and update cascade
drug_services = relationship("drug_services", cascade="all,delete", backref="providers")



# Map providers_misc_id
drug_services_id_list = [drug_services.drug_services_id[drug_services.number_of_hcpcs_associated_with_drug_services == i].values[0] for i in df.number_of_hcpcs_associated_with_drug_services]

# Add name_id to the main dataframe
df.insert(27, 'drug_services_id', drug_services_id_list)


#################medical_services#################



# Subset relevant columns to get medical_services table
medical_services = df[['npi','medical_suppress_indicator','number_of_hcpcs_associated_with_medical_services','number_of_medical_services','number_of_unique_beneficiaries_with_medical_services']]



# Drop duplicates and NA
medical_services = medical_services.drop_duplicates()
medical_services = medical_services.dropna()

# Add index
medical_services.insert(0, 'medical_services_id', range(1, 1 + len(medical_services)))



# Change # with Y, meaning this medical service has been suppressed, NA with N, meaning no suppression existists.
medical_services['medical_suppress_indicator'] = medical_services.medical_suppress_indicator.fillna('N')
medical_services.medical_suppress_indicator[medical_services.medical_suppress_indicator == '#'] = 'Y'



# Rename to fit the schema
medical_services.columns = ['medical_services_id','npi','medical_suppress_indicator','number_of_hcpcs_medical_services','number_medical_services','number_of_medicare_beneficiaries_with_medical_services']



# Populate the database
medical_services[['medical_services_id','npi','medical_suppress_indicator','number_of_hcpcs_medical_services','number_medical_services','number_of_medicare_beneficiaries_with_medical_services']].     drop_duplicates().to_sql(name='medical_services', con=engine, if_exists='append', index=False)



# Add delete and update cascade
medical_services = relationship("medical_services", cascade="all,delete", backref="providers")



# Map providers_misc_id
medical_services_id_list = [medical_services.medical_services_id[medical_services.number_of_hcpcs_medical_services == i].values[0] for i in df.number_of_hcpcs_associated_with_medical_services]

# Add name_id to the main dataframe
df.insert(27, 'medical_services_id', medical_services_id_list)


