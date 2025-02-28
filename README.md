# fe5_anaphylaxis
 cases from unstructured text data in electronic health records.

## Overview
This Python-based pipeline is used for identifying and extracting anaphylaxis cases based on ICD codes and encounter note types (inpatient, outpatient, ED visits).

## Pipeline Structure
1. Reads two SAS files (for 2019 and 2020)
2. Decodes byte‐type columns and converts the Admit_Date column to datetime.
*3. Classifies the encounter type into Inpatient, ED, Outpatient, or Other.
4. Creates indicator columns for whether the row’s diagnosis (Dx) falls into List 1, List 2 (group I and II), or List 3. **Note: For the Dx that belongs to List 1, 2, and 3, please refer to [Here](./Cohort%20identification%20of%20potential%20anaphylaxis%20events.pdf)**
5. Implements three criteria (“paths”) for anaphylaxis:
    • Path 1 (“Wash criterion A”): For an inpatient/ED row with a List 1 code, check that no prior encounter (within 60 days) for the same patient had the same List 1 code.
    • Path 2 (“Walsh criterion B”): For an outpatient row with a List 1 code, check that on the same calendar day the patient has at least one List 2 code (Group I or II).
    • Path 3 (“Walsh criterion C”): For an ED or inpatient row with a List 3 code, check that on the same day (for ED) or within the same encounter (for inpatient) there is at least one List 2 code from Group I and one from Group II.
6. Then, at the encounter level (i.e. for every [PATIENT_NUM, ENCOUNTER_NUM] pair) the script marks Feature_Status = "A" if any row meets any of the three criteria and "U" otherwise.
7. Finally, it outputs a CSV file with the required columns and fixed values for some.

*Below are the rules of classifying the encounter type:
• Emergency department encounter includes: ED
• Inpatient encounter includes: EI, IP
• Outpatient encounter includes: AV, IS, OS, IC, TH, OA

where the abbreviations are defined as follows:
• AV=Ambulatory  Visit
• ED=Emergency  Department
• EI=Emergency  Department  Admit  to  Inpatient  Hospital  Stay  (permissible  substitution)
• IP=Inpatient  Hospital  Stay
• IS=Non-Acute  Institutional  Stay
• OS=Observation  Stay
• IC=Institutional  Professional  Consult  (permissible  substitution)
• TH=Telehealth
• OA=Other  Ambulatory  Visit
• NI=No  information
• UN=Unknown
• OT=Other