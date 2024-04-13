

def createExcelFile(obd2_data_frame):

    try:
        time_diff = obd2_data_frame['storage_time'] - obd2_data_frame['tx_time']
        
        # Convert time difference to seconds (assuming all values are valid)
        obd2_data_frame['time_diff_seconds'] = time_diff.dt.total_seconds()
            
        # Generate Excel report
        obd2_data_frame.to_excel("obd2_data_report.xlsx", index=False)
        print("Excel file has been created.")
    
    except Exception as e:
        print(f"Failed to create excel file: {e}")