import numpy as np
import pandas as pd
from datetime import date

def main():
    feature_list = ["anaphylaxis"]
    
    for feature in feature_list:
        # Read the CSV file
        df = pd.read_csv(f"fe_feature_detail_table_{feature}.csv")

        # Group the rows and aggregate the Feature_Status values of different NoteIDs by the order
        order = {"A": 5, "N": 4, "H": 3, "X": 2, "U": 1}
        df_grouped_status = df.groupby(["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"]).agg({"Feature_Status": lambda x: max(x, key=lambda y: order[y])}).reset_index()

        # Group the rows and aggregate the Feature_dt and ProviderID of different NoteIDs by the earliest date
        df_grouped_date = df.groupby(["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"]).agg({"Feature_dt": "min", "ProviderID": "first"}).reset_index()

        # Merge the two DataFrames
        df = pd.merge(df_grouped_date, df_grouped_status, on=["PatID", "EncounterID", "FeatureID", "Feature", "FE_CodeType", "Confidence"], how="inner")

        # Ensure that the columns are in the order of the original table
        df = df[["PatID", "EncounterID", "FeatureID", "Feature_dt", "Feature", "FE_CodeType", "ProviderID", "Confidence", "Feature_Status"]]

        # Sort the rows by PatID and EncounterID
        df.sort_values(by=["PatID", "EncounterID"], inplace=True)

        df.reset_index(drop=True)

        if feature == "anaphylaxis":
            df["FeatureID"] = df.index.map(lambda x: f"1004{x:08d}")

            featureid_df = pd.DataFrame()
            featureid_df["FeatureID"] = df["FeatureID"]

            name = "cTAKES pipeline - anaphylaxis"
            version = "1.0.0"
            run_date = date.today()
            description = "Rule-based pipeline to extract anaphylaxis status."
            source = "https://github.com/YLab-Open/fe5_cTAKES"

            featureid_df['Feature'] = name
            featureid_df['Version'] = version
            featureid_df['Run_Date'] = run_date
            featureid_df['Description'] = description
            featureid_df['Source'] = source
            
            featureid_df_output_file = "fe_pipeline_table_anaphylaxis.csv"
            featureid_df.to_csv(featureid_df_output_file, index=False)
            print("Pipeline information CSV saved to:", featureid_df_output_file)

        df.to_csv(f"fe_feature_table_{feature}.csv", index=False)

        print(f"Saved fe_feature_table_{feature}.csv")

    print("Final results generated for anaphylaxis. Final results saved as fe_feature_table_anaphylaxis.csv. Post Processing Step 3 complete.")

if __name__ == "__main__":
    main()