#!/usr/bin/env python3
"""
Anaphylaxis identification pipeline

This script:
  1. Reads two SAS files (for 2019 and 2020)
  2. Decodes byte‐type columns and converts the Admit_Date column to datetime.
  3. Classifies the encounter type into Inpatient, ED, Outpatient, or Other.
  4. Creates indicator columns for whether the row’s diagnosis (Dx) falls into List 1,
     List 2 (group I and II), or List 3.
  5. Implements three criteria (“paths”) for anaphylaxis:
       • Path 1 (“Wash criterion A”): For an inpatient/ED row with a List1 code, check that no 
         prior encounter (within 60 days) for the same patient had the same List1 code.
       • Path 2 (“Walsh criterion B”): For an outpatient row with a List1 code, check that on the 
         same calendar day the patient has at least one List2 code (Group I or II).
       • Path 3 (“Walsh criterion C”): For an ED or inpatient row with a List3 code, check that on the 
         same day (for ED) or within the same encounter (for inpatient) there is at least one List2 code 
         from Group I and one from Group II.
  6. Then, at the encounter level (i.e. for every [PATIENT_NUM, ENCOUNTER_NUM] pair) the script marks 
     Feature_Status = "A" if any row meets any of the three criteria and "U" otherwise.
  7. Finally, it outputs a CSV file with the required columns and fixed values for some.
     
The final CSV file is saved at:
  ../Result/fe_feature_table_anaphylaxis.csv
"""

import pandas as pd
import numpy as np
from datetime import timedelta, date

###########################
# 1. Read and concatenate data
###########################
file2019 = "../Data/diagnosis_2019.sas7bdat"
file2020 = "../Data/diagnosis_2020.sas7bdat"

df2019 = pd.read_sas(file2019)
df2020 = pd.read_sas(file2020)
df = pd.concat([df2019, df2020], ignore_index=True)

###########################
# 2. Decode byte columns and convert Admit_Date
###########################
# Columns stored as bytes: Dx_Type, Dx, PROVIDER_ID, Enc_Type.
for col in ['Dx_Type', 'Dx', 'PROVIDER_ID', 'Enc_Type']:
    df[col] = df[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

df['Admit_Date'] = pd.to_datetime(df['Admit_Date'])
df = df[df['Admit_Date'].dt.year.isin([2019, 2020])]  # Ensure only 2019 and 2020 data is included

###########################
# 3. Classify encounter type
###########################
def classify_encounter(enc):
    if enc in ['EI', 'IP']:
        return 'Inpatient'
    elif enc == 'ED':
        return 'ED'
    elif enc in ['AV', 'IS', 'OS', 'IC', 'TH', 'OA']:
        return 'Outpatient'
    else:
        return 'Other'
        
df['Encounter_Class'] = df['Enc_Type'].apply(classify_encounter)

###########################
# 4. Define the diagnosis code lists (without dots)
###########################

# List 1
list1_icd9 = {"9950", "99560", "99561", "99562", "99565", "99563", "99564", "99566",
              "99567", "99568", "99569", "99941", "99942", "99949"}
list1_icd10 = {"T782XXA", "T886XXA", "T7800XA", "T7801XA", "T7802XA", "T7803XA",
               "T7804XA", "T7805XA", "T7806XA", "T7807XA", "T7808XA", "T7809XA",
               "T8051XA", "T8052XA", "T8059XA"}

# List 2 (Group I)
list2_group1_icd9 = {"51911", "7861", "J1200"}
list2_group1_icd10 = {"J9801", "R061", "J1200"}

# List 2 (Group II)
list2_group2_icd9 = {"4589", "J0170", "J0171", "92950", "9960"}  # 99.60 is stored as "9960"
list2_group2_icd10 = {"I959", "J0170", "J0171", "92950", "5A12012", "5A1221Z"}

# List 3
list3_icd9 = {"9952", "99522", "99523", "99527", "99529", "9953"}
# For ICD-9, also any code in the range E930.* - E949.* will qualify.
# For ICD-10, we list the many explicit codes:
list3_icd10 = {
    "T50905A", "T410X5A", "T411X5A", "T41205A", "T41295A", "T413X5A", "T4145XA", "T8859XA",
    "T383X5A", "T50995A", "T7840XA", "T7849XA", "T360X5A", "T361X5A", "T362X5A", "T363X5A",
    "T364X5A", "T365X5A", "T366X5A", "T367X5A", "T368X5A", "T3695XA", "T370X5A", "T371X5A",
    "T372X5A", "T373X5A", "T374X5A", "T375X5A", "T378X5A", "T3795XA", "T380X5A", "T381X5A",
    "T382X5A", "T384X5A", "T385X5A", "T386X5A", "T387X5A", "T38805A", "T38815A", "T38895A",
    "T38905A", "T38995A", "T39015A", "T39095A", "T391X5A", "T392X5A", "T39315A", "T39395A",
    "T394X5A", "T398X5A", "T3995XA", "T400X5A", "T402X5A", "T403X5A", "T404X5A", "T405X5A",
    "T40605A", "T40695A", "T407X5A", "T40905A", "T40995A", "T415X5A", "T420X5A", "T421X5A",
    "T422X5A", "T423X5A", "T424X5A", "T425X5A", "T426X5A", "T4275XA", "T428X5A", "T43015A",
    "T43025A", "T431X5A", "T43205A", "T43215A", "T43225A", "T43295A", "T433X5A", "T434X5A",
    "T43505A", "T43595A", "T43605A", "T43615A", "T43625A", "T43635A", "T43695A", "T438X5A",
    "T4395XA", "T440X5A", "T441X5A", "T442X5A", "T442X5S", "T443X5A", "T444X5A", "T445X5A",
    "T446X5A", "T447X5A", "T448X5A", "T44905A", "T44905S", "T44995A", "T44995S", "T450X5A",
    "T451X5A", "T452X5A", "T453X5A", "T454X5A", "T45515A", "T45525A", "T45605A", "T45615A",
    "T45625A", "T45695A", "T457X5A", "T458X5A", "T4595XA", "T460X5A", "T461X5A", "T462X5A",
    "T463X5A", "T464X5A", "T465X5A", "T466X5A", "T467X5A", "T468X5A", "T46905A", "T46995A",
    "T470X5A", "T471X5A", "T472X5A", "T473X5A", "T474X5A", "T475X5A", "T476X5A", "T477X5A",
    "T478X5A", "T4795XA", "T480X5A", "T481X5A", "T48205A", "T48295A", "T483X5A", "T484X5A",
    "T485X5A", "T486X5A", "T48905A", "T48995A", "T490X5A", "T491X5A", "T492X5A", "T493X5A",
    "T494X5A", "T495X5A", "T496X5A", "T497X5A", "T498X5A", "T4995XA", "T500X5A", "T501X5A",
    "T502X5A", "T503X5A", "T504X5A", "T505X5A", "T506X5A", "T507X5A", "T508X5A", "T50905A",
    "T50A15A", "T50A25A", "T50A95A", "T50B15A", "T50B95A", "T50Z15A", "T50Z95A", "T887XXA"
}

###########################
# 5. Create indicator columns for each list (taking into account the Dx_Type)
###########################

def in_list1(row):
    code = row['Dx']
    if row['Dx_Type'] == '09':
        return code in list1_icd9
    elif row['Dx_Type'] == '10':
        return code in list1_icd10
    else:
        return False

def in_list2_group1(row):
    code = row['Dx']
    if row['Dx_Type'] == '09':
        return code in list2_group1_icd9
    elif row['Dx_Type'] == '10':
        return code in list2_group1_icd10
    else:
        return False

def in_list2_group2(row):
    code = row['Dx']
    if row['Dx_Type'] == '09':
        return code in list2_group2_icd9
    elif row['Dx_Type'] == '10':
        return code in list2_group2_icd10
    else:
        return False

def in_list3(row):
    code = row['Dx']
    if row['Dx_Type'] == '09':
        # either an explicit code or a code in the range E930.* - E949.*
        if code in list3_icd9:
            return True
        if code.startswith("E") and len(code) >= 4:
            try:
                # extract the 3 digits immediately after "E" and check if between 930 and 949
                num = int(code[1:4])
                if 930 <= num <= 949:
                    return True
            except:
                return False
        return False
    elif row['Dx_Type'] == '10':
        return code in list3_icd10
    else:
        return False

df['in_list1'] = df.apply(in_list1, axis=1)
df['in_list2_group1'] = df.apply(in_list2_group1, axis=1)
df['in_list2_group2'] = df.apply(in_list2_group2, axis=1)
df['in_list3'] = df.apply(in_list3, axis=1)

# For Path 2 we need a flag for any List2 code (either group)
df['in_list2_any'] = df['in_list2_group1'] | df['in_list2_group2']

###########################
# 6. Implement the three criteria (paths)
###########################

# --------- Path 1 (Wash criterion A) -----------
# For an inpatient/ED row with a List1 code, ensure that in the preceding 60 days the same patient did not have
# any encounter (any setting) with the same List1 code.
# Initialize the flag for Path 1 to False for all rows.
df['flag_path1'] = False

# We first restrict to all rows where the diagnosis is in List 1.
mask_list1 = df['in_list1']

# Now, for each patient and each diagnosis code (List 1 code),
# consider all encounters (from any setting) for that patient with that code.
# Then, for each candidate encounter (which must be Inpatient or ED),
# check if there is any encounter in the 60 days immediately preceding it.
for (patient, code), group in df[mask_list1].groupby(['PATIENT_NUM', 'Dx']):
    # Sort the group by Admit_Date in ascending order.
    group = group.sort_values('Admit_Date')
    for idx, row in group.iterrows():
        # We only consider the current row as a candidate if it is an Inpatient or ED encounter.
        if row['Encounter_Class'] in ['Inpatient', 'ED']:
            # Get all encounters (for the same patient and same List 1 code)
            # that occurred before the current encounter.
            prev_encounters = group[group['Admit_Date'] < row['Admit_Date']]
            # If there is no previous encounter, then the candidate is preceded by a clean washout period.
            if prev_encounters.empty:
                df.at[idx, 'flag_path1'] = True
            else:
                # Determine the most recent encounter date among those prior encounters.
                last_date = prev_encounters['Admit_Date'].max()
                # Calculate the difference in days between the current encounter and the most recent prior encounter.
                day_diff = (row['Admit_Date'] - last_date).days
                # If the candidate encounter is at least 60 days after the most recent encounter, it qualifies.
                if day_diff >= 60:
                    df.at[idx, 'flag_path1'] = True
                else:
                    # Otherwise, there is an encounter within the 60-day lookback period.
                    df.at[idx, 'flag_path1'] = False
            
# --------- Path 2 (Walsh criterion B) -----------
# For an outpatient encounter with a List1 code, check that on the same calendar day (i.e. for the same patient and Admit_Date)
# there is at least one diagnosis code in List2 (either group).
# First, determine for each (PATIENT_NUM, Admit_Date) whether any row has a List2 code.
list2_day = df.groupby(['PATIENT_NUM', 'Admit_Date'])['in_list2_any'].any().reset_index().rename(
    columns={'in_list2_any': 'has_list2'})
# Ensure Admit_Date is datetime in list2_day before merge
list2_day['Admit_Date'] = pd.to_datetime(list2_day['Admit_Date'])
# Merge this flag back to the main data frame.
df = df.merge(list2_day, on=['PATIENT_NUM', 'Admit_Date'], how='left')
df['has_list2'] = df['has_list2'].fillna(False)

df['flag_path2'] = False
mask_path2 = (df['Encounter_Class'] == 'Outpatient') & (df['in_list1'])
df.loc[mask_path2, 'flag_path2'] = df.loc[mask_path2, 'has_list2']

# --------- Path 3 (Walsh criterion C) -----------
# For an ED or inpatient encounter with a List3 code:
#   - If inpatient, then within the same encounter (same ENCOUNTER_NUM) there must be at least one List2 group1 code and one List2 group2 code.
#   - If ED, then within the same calendar day (PATIENT_NUM + Admit_Date) the same must be true.
df['flag_path3'] = False

# For inpatient encounters:
inpt = df[df['Encounter_Class'] == 'Inpatient']
inpt_grp = inpt.groupby(['PATIENT_NUM', 'ENCOUNTER_NUM']).agg({
    'in_list2_group1': 'any',
    'in_list2_group2': 'any',
    'in_list3': 'any'
}).reset_index()
inpt_grp['flag_path3'] = inpt_grp['in_list3'] & inpt_grp['in_list2_group1'] & inpt_grp['in_list2_group2']
# Merge the inpatient flag back
df = df.merge(inpt_grp[['PATIENT_NUM', 'ENCOUNTER_NUM', 'flag_path3']],
              on=['PATIENT_NUM', 'ENCOUNTER_NUM'], how='left', suffixes=("", "_inpt"))
df['flag_path3'] = df['flag_path3'].fillna(False)

# For ED encounters (group by patient and Admit_Date):
ed = df[df['Encounter_Class'] == 'ED']
ed_grp = ed.groupby(['PATIENT_NUM', 'Admit_Date']).agg({
    'in_list2_group1': 'any',
    'in_list2_group2': 'any',
    'in_list3': 'any'
}).reset_index()
ed_grp['flag_path3'] = ed_grp['in_list3'] & ed_grp['in_list2_group1'] & ed_grp['in_list2_group2']
# Ensure Admit_Date is datetime in ed_grp before merging
ed_grp['Admit_Date'] = pd.to_datetime(ed_grp['Admit_Date'])
# Merge the ED flag back (use a different name to avoid overwriting)
ed_grp = ed_grp[['PATIENT_NUM', 'Admit_Date', 'flag_path3']].rename(columns={'flag_path3': 'flag_path3_ed'})
df = df.merge(ed_grp, on=['PATIENT_NUM', 'Admit_Date'], how='left')
df['flag_path3_ed'] = df['flag_path3_ed'].fillna(False)
# For ED rows, override the flag_path3 with the ED version.
df.loc[df['Encounter_Class'] == 'ED', 'flag_path3'] = df.loc[df['Encounter_Class'] == 'ED', 'flag_path3_ed']

###########################
# 7. Determine overall qualification at the row level
###########################
df['qualifies'] = df['flag_path1'] | df['flag_path2'] | df['flag_path3']

###########################
# 8. Aggregate to the encounter level (all (PATIENT_NUM, ENCOUNTER_NUM) pairs from the two files)
###########################
# For each encounter, we take the (first) Admit_Date and PROVIDER_ID (assumed constant per encounter)
encounter_df = df.groupby(['PATIENT_NUM', 'ENCOUNTER_NUM', 'Admit_Date', 'PROVIDER_ID'], as_index=False)['qualifies'].max()
encounter_df['Feature_Status'] = encounter_df['qualifies'].apply(lambda x: "A" if x else "U")
# Ensure the Admit_Date remains a proper datetime before renaming it
encounter_df['Admit_Date'] = pd.to_datetime(encounter_df['Admit_Date'])
# Rename Admit_Date to Feature_dt
encounter_df.rename(columns={'Admit_Date': 'Feature_dt'}, inplace=True)

# Add constant columns as specified.
encounter_df['FeatureID']   = 1001
encounter_df['Feature']     = "C0002792"
encounter_df['FE_CodeType'] = "UC"
encounter_df['Confidence']  = "N"

# Rename remaining columns.
encounter_df.rename(columns={
    'PATIENT_NUM': 'PatID',
    'ENCOUNTER_NUM': 'EncounterID',
    'PROVIDER_ID': 'ProviderID'
}, inplace=True)

# Group the rows and aggregate the Feature_Status values of different NoteIDs by the order
order = {"A": 2, "U": 1}
df_grouped_status = encounter_df.groupby(["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"]).agg({"Feature_Status": lambda x: max(x, key=lambda y: order[y])}).reset_index()

# Group the rows and aggregate the Feature_dt and ProviderID of different NoteIDs by the earliest date
df_grouped_date = encounter_df.groupby(["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"]).agg({"Feature_dt": "min", "ProviderID": "first"}).reset_index()

# Merge the two DataFrames
encounter_df = pd.merge(df_grouped_date, df_grouped_status, on=["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"], how="inner")

# Ensure that the columns are in the order of the original table
final_df = encounter_df[["PatID", "EncounterID", "FeatureID", "Feature_dt", "Feature", "FE_CodeType", "ProviderID", "Confidence", "Feature_Status"]]

# Sort the rows by PatID
final_df = final_df.sort_values('PatID')

###########################
# 9. Write the final CSV file
###########################
output_file = "../Result/fe_feature_table_anaphylaxis.csv"
final_df.to_csv(output_file, index=False)

print("Pipeline complete. CSV saved to:", output_file)

###########################
# 10. Generate the pipeline information for FeatureID
###########################
id = 1001
name = "anaphylaxis pipeline"
version = "1.0.0"
run_date = date.today()
description = "Regular expression-based pipeline to extract anaphylaxis"
source = "https://github.com/YLab-Open/fe5_anaphylaxis"

featureid_df = pd.DataFrame({
    'FeatureID': [id],
    'Feature': [name],
    'Version': [version],
    'Run_Date': [run_date],
    'Description': [description],
    'Source': [source]
})

featureid_df_output_file = "../Result/fe_pipeline_table.csv"
featureid_df.to_csv(featureid_df_output_file, index=False)
print("Pipeline information CSV saved to:", featureid_df_output_file)