# fe5_anaphylaxis
The FE5 Anaphylaxis pipeline.

## Overview
This Python-based pipeline is used for identifying and extracting anaphylaxis cases based on diagnosis and procedure codes, encounter note types (inpatient, outpatient, ED visits), and patient EHR notes.

## Pipeline Structure
This pipeline involves three main steps:

A. [Pre-processing](./anaphylaxis_preprocessing.ipynb)

1. Reads four SAS files that contains the disgnosis, procedure codes, and encounter note types for 2019 and 2020
2. Classifies the encounter type into Inpatient, ED, Outpatient, or Other.
3. Implements three criteria (“paths”) for anaphylaxis (for details, please refer to [Here](./Cohort%20identification%20of%20potential%20anaphylaxis%20events.pdf)):

    • Path 1 (“Wash criterion A”): For an inpatient/ED row with a List 1 code, check that no prior encounter (within 60 days) for the same patient had the same List 1 code.
   
    • Path 2 (“Walsh criterion B”): For an outpatient row with a List 1 code, check that on the same calendar day the patient has at least one List 2 code (Group I or II).
   
    • Path 3 (“Walsh criterion C”): For an ED or inpatient row with a List 3 code, check that on the same day (for ED) or within the same encounter (for inpatient) there is at least one List 2 code from Group I and one from Group II.
4. The target patient cohort is defined as patients with at least one encounter that meets any of the three criteria.
5. The output of this step is a CSV file that contains the patient_id, note_id, encounter_id, note_date, provider_id, and the note_context

B. [Assign feature status for anaphylaxis using cTAKES](./fe5_cTAKES)

Based on the CSV file generated through step A, the pipeline uses the [fe5_cTAKES pipeline](https://github.com/YLab-Open/fe5_cTAKES/tree/main), to extract anaphylaxis-related features from the EHR notes of the target patients. The CUIs used for anaphylaxis is available [Here](./fe5_cTAKES/CUI/anaphylaxis_umls_cui_clean.txt)

C. [Post-processing](./anaphylaxis_postprocessing.ipynb)

Based on the output of step B, this step generates an encounter-level result that aggregates the note-level features extracted from the EHR notes of the target patients. The reason of using a standalone post-processing is that for anaphylaxis, we only have two feature status code (A or U), which is different from the original fe5_cTAKES pipeline, so any non-A feature status needs be reassigned to U before aggregtion.

## How the encounter type is determined
*Below are the rules of classifying the encounter type:

• **Emergency** department encounter includes: **ED**

• **Inpatient** encounter includes: **EI, IP**

• **Outpatient** encounter includes: **AV, IS, OS, IC, TH, OA**

• All **other** types are classified as **"Other"**

The abbreviations are defined as follows:

• **AV**=Ambulatory  Visit

• **ED**=Emergency  Department

• **EI**=Emergency  Department  Admit  to  Inpatient  Hospital  Stay  (permissible  substitution)

• **IP**=Inpatient  Hospital  Stay

• **IS**=Non-Acute  Institutional  Stay

• **OS**=Observation  Stay

• **IC**=Institutional  Professional  Consult  (permissible  substitution)

• **TH**=Telehealth

• **OA**=Other  Ambulatory  Visit

• **NI**=No  information

• **UN**=Unknown

• **OT**=Other
