USE PEOPLE_ODS
GO

IF OBJECT_ID ('dbo.LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT') IS NOT NULL
DROP PROCEDURE [dbo].[LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT]
GO


CREATE PROCEDURE [dbo].[LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT]
AS
BEGIN
----------------------------------------------------------------------------------------------------------------------------------------------------
--  Name              : LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT
--  Purpose           : 1. Synchronize between 
--                         JOB_APP and WDJobApplicationExtract
--                      2. It Synchronize only current month (SRC_FILE_Date) records from HrdwStaging..WDJobApplicationExtract. All the source views has filter to get current SRC_FILE_DATE
--							records.
--	Sample Call       : EXEC [PEOPLE_ODS].[dbo].[LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT]					
--
--  Input Parameters  : NONE
--
--  Output Paramaters : NONE
--
--  Version           : 1.0
--  Date              : 04/30/2019
--  Author            : hkathi
--   
----------------------------------------------------------------------------------------------------------------------------------------------------
--
-- Modification History
--
-- ModIfication : 
-- Version      : 									
-- Date         : 
-- Author       : 
--

----------------------------------------------------------------------------------------------------------------------------------------------------
BEGIN TRY
	DECLARE  @JOB_APP_SRC_SYS_ID		SMALLINT
	DECLARE  @JOB_APP_SUB_SRC_SYS_ID	SMALLINT
	DECLARE  @TARGET_TBL_NAME		    VARCHAR(250) = 'DISPOSTN_REASN_TYP'
	DECLARE  @SRC_TBL_NAME_JOB_APP		VARCHAR(250) = 'WDJobApplicationExtract'
	DECLARE  @SRC_SYS_NAME			    VARCHAR(250) = 'Workday'
	DECLARE  @SUB_SRC_SYS_NAME		    VARCHAR(250) = 'Not Applicable'
	DECLARE  @ETL_CREATE_DATETM		    DATETIME2 = GETDATE()
	DECLARE  @ETL_CREATE_EMP_LOGIN_NAME NVARCHAR(255)= SUSER_SNAME()
	DECLARE  @ETL_UPDATE_DATETM		    DATETIME2 = GETDATE()
	DECLARE  @ETL_UPDATE_EMP_LOGIN_NAME NVARCHAR(255)= SUSER_SNAME()
	DECLARE	 @CurrentTime			    DATETIME2	= GETDATE() --Declare the variables
	DECLARE	 @RecordsInserted		    INT			= 0		-- used to return values after sproc completes
	DECLARE	 @RecordsUpdated		    INT			= 0		-- used to return values after sproc completes
	DECLARE	 @RecordsDeleted		    INT			= 0		-- used to return values after sproc completes
	DECLARE	 @msg					    VARCHAR(255)		-- used for error messaging
	DECLARE	 @spname				    VARCHAR(255)		-- used for error messaging
	DECLARE	 @ErrorText				    VARCHAR(MAX)		-- used for error messaging
	DECLARE	 @ErrorNum				    INT					-- used for error messaging
	DECLARE	 @StagingDataSetID		    INT					-- ID of data set in staging table
	DECLARE	 @ExtractedDATETIME		    DATETIME 
	DECLARE	 @MergeRecordCount		    INT			= 0
	DECLARE	 @HRDWRecordCount		    INT			= 0
	DECLARE	 @TotalRecords              INT			= 0 
	DECLARE	 @ExceptionRowCount		    INT 
	DECLARE  @SRC_FILE_DATE_JOB_APP	    DATE
	DECLARE  @SRC_FILE_REC_CNT_JOB_APP	INT
	DECLARE	 @SRC_CREATE_DATETM_JOB_APP DATETIME
	DECLARE  @EVENTTIME				    DATETIME2 = GETDATE()
	DECLARE	 @RCNT					    INT
	SELECT @spname = OBJECT_NAME(@@procid);

	EXEC PEOPLE_ODS.[dbo].[ETL_TABLE_COMPARISON]	@TARGET_TBL_NAME = @TARGET_TBL_NAME
													,@SRC_SYS_NAME  = @SRC_SYS_NAME
													,@SRC_TBL_NAME  = @SRC_TBL_NAME_JOB_APP 
													,@SourceExtractedDate = @SRC_CREATE_DATETM_JOB_APP OUTPUT
													,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP OUTPUT
													,@SRC_FILE_REC_CNT = @SRC_FILE_REC_CNT_JOB_APP OUTPUT
	
	IF @SRC_FILE_DATE_JOB_APP IS NULL
		GOTO FINISH

	SET @EVENTTIME = GETDATE()
	EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
										,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
										,@EventName='START'
										,@EVENT_SQL_STATEMENT='EXEC [dbo].[ETLTableComparison] @TARGET_TBL_NAME = @TARGET_TBL_NAME,	@SRC_SYS_NAME =@SRC_SYS_NAME,@SRC_TBL_NAME =@SRC_TBL_NAME , @SourceExtractedDate=@SRC_CREATE_DATETM OUTPUT,@SRC_FILE_DATE=@SRC_FILE_DATE OUTPUT,@SRC_FILE_REC_CNT=@SRC_FILE_REC_CNT OUTPUT'
										,@Message = ''
										,@EVENT_TIME = @EVENTTIME
										,@RecordsAffected = 0
										,@RecordsInserted = 0
										,@RecordsUpdated  = 0

	DECLARE @MergeResult TABLE 
	(	MergeAction VARCHAR(150)
		,DISPOSTN_REASN_TYP_NAME	VARCHAR(510)
	)
	BEGIN TRY
		
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
											,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
											,@EventName='START'
											,@EVENT_SQL_STATEMENT='SELECT * INTO  #DISPOSTN_REASN_TYP FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
											,@Message='LOADING DATA INTO SESSION TABLE WITH ALL DATA TYPE CONVERSIONS IS STARTED'
											,@EVENT_TIME=@EVENTTIME
											,@RecordsAffected = 0
											,@RecordsInserted = 0
											,@RecordsUpdated  = 0
		IF OBJECT_ID ('tempdb..#DISPOSTN_REASN_TYP') IS NOT NULL
		DROP TABLE #DISPOSTN_REASN_TYP;	

		SELECT DISTINCT dbo.TrimBlankNull(DISPOSITIONREASON) AS DISPOSTN_REASN_TYP_NAME
		INTO #DISPOSTN_REASN_TYP
		FROM [HRDWStaging].[dbo].[WDJOBAPPLICATIONEXTRACT] WITH (NOLOCK) 
		WHERE dbo.TrimBlankNull(DISPOSITIONREASON) IS NOT NULL 
		AND  SourceExtractedDATE =  @SRC_FILE_DATE_JOB_APP
		
		SELECT @RCNT= @@ROWCOUNT
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
											,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
											,@EventName='END'
											,@EVENT_SQL_STATEMENT='SELECT * INTO  #DISPOSTN_REASN_TYP FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
											,@Message='SUCCEEDED: LOADING DATA INTO SESSION TABLE WITH ALL DATA TYPE CONVERSIONS IS COMPLETED'
											,@EVENT_TIME=@EVENTTIME
											,@RecordsAffected = @RCNT
											,@RecordsInserted = 0
											,@RecordsUpdated  = 0

	END TRY
	BEGIN CATCH
	    SELECT
			    @ErrorNum = ERROR_NUMBER()
			    ,@ErrorText = ERROR_MESSAGE()
		SELECT	@msg = 'FAILED: @@Error in Proc ' + @spname + ': ' + ISNULL(CONVERT(VARCHAR,@ErrorNum),0) + ' ' + @ErrorText
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'ERROR'
                                            ,@EVENT_SQL_STATEMENT = 'SELECT * INTO  #DISPOSTN_REASN_TYP FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
			                                ,@Message = @msg
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		RAISERROR (@msg,16,127) WITH NOWAIT
		RETURN -1;
	END CATCH
    BEGIN TRY
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
                                            ,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
                                            ,@EventName='START'
                                            ,@EVENT_SQL_STATEMENT = ' MERGE  PEOPLE_ODS.DBO.EEO_TYP AS DEST 
																		USING #DISPOSTN_REASN_TYP AS SRC 
																		ON UPPER(DEST.DISPOSTN_REASN_TYP_NAME)	= UPPER(SRC.DISPOSTN_REASN_TYP_NAME) '
                                            ,@Message='STARTED: LOADING DATA INTO "PEOPLE_ODS.DBO.JOB_APP" TABLE WITH INSERT/UPDATE'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		BEGIN TRAN
			MERGE  PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP AS DEST 
	        USING #DISPOSTN_REASN_TYP AS SRC 
	        ON UPPER(DEST.DISPOSTN_REASN_TYP_NAME)	= UPPER(SRC.DISPOSTN_REASN_TYP_NAME)		
            WHEN NOT MATCHED BY TARGET THEN
            INSERT 
            (
				 DISPOSTN_REASN_TYP_NAME        
                ,ETL_CREATE_DATETM
                ,ETL_CREATE_EMP_LOGIN_NAME
                ,ETL_UPDATE_DATETM
                ,ETL_UPDATE_EMP_LOGIN_NAME
            )
            VALUES 
            (
				 SRC.DISPOSTN_REASN_TYP_NAME        
                ,@ETL_CREATE_DATETM
                ,@ETL_CREATE_EMP_LOGIN_NAME
                ,@ETL_UPDATE_DATETM
                ,@ETL_UPDATE_EMP_LOGIN_NAME
	        ) 
            OUTPUT $ACTION AS MergeAction,	
			            SRC.DISPOSTN_REASN_TYP_NAME	
                        INTO @MergeResult;

				SELECT @RecordsInserted = @RecordsInserted + COUNT(*) FROM @MergeResult WHERE MergeAction = 'INSERT';
                SELECT @RecordsUpdated = @RecordsUpdated + COUNT(*)FROM @MergeResult WHERE MergeAction = 'UPDATE';
                SET @MergeRecordCount = @RecordsInserted + @RecordsUpdated
				SET @EVENTTIME = GETDATE()
                EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                                    ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                                    ,@EventName = 'END'
                                                    ,@EVENT_SQL_STATEMENT = ' MERGE  PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP AS DEST 
																			 USING #DISPOSTN_REASN_TYP AS SRC 
																			 ON UPPER(DEST.DISPOSTN_REASN_TYP_NAME)	= UPPER(SRC.DISPOSTN_REASN_TYP_NAME)  '
                                                    ,@Message = 'SUCCEDED: LOADING DATA INTO "PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP" TABLE WITH INSERT/UPDATE'
                                                    ,@EVENT_TIME = @EVENTTIME
                                                    ,@RecordsAffected = @MergeRecordCount
                                                    ,@RecordsInserted = @RecordsInserted
                                                    ,@RecordsUpdated  = @RecordsUpdated
				
                SET @EVENTTIME = GETDATE()
                EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                                    ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                                    ,@EventName = 'START'
                                                    ,@EVENT_SQL_STATEMENT = 'INSERT INTO [ETL].[ODS_ETL_CTRL_TBL] ([TARGET_TBL_NAME],[TARGET_TBL_LOAD_DATETM],[SRC_TBL_NAME],[SRC_TBL_LOAD_DATETM],[SRC_FILE_DATE],[SRC_TBL_REC_CNT]) VALUES (@TARGET_TBL_NAME,@CurrentTime,@SRC_TBL_NAME,@SRC_CREATE_DATETM,@SRC_FILE_DATE,@SRC_FILE_REC_CNT)'
                                                    ,@Message = 'STARTED: LOGGING THE RECORD INTO "[ETL].[ODS_ETL_CTRL_TBL] " TABLE '
                                                    ,@EVENT_TIME = @EVENTTIME
                                                    ,@RecordsAffected = 0
                                                    ,@RecordsInserted = 0
                                                    ,@RecordsUpdated  = 0
				MERGE PEOPLE_ODS.ETL.ODS_ETL_CTRL_TBL TGT
				USING (SELECT @TARGET_TBL_NAME AS TARGET_TBL_NAME
					,@CurrentTime AS TARGET_TBL_LOAD_DATETM
					,@SRC_TBL_NAME_JOB_APP	AS SRC_TBL_NAME
					,@SRC_CREATE_DATETM_JOB_APP AS SRC_TBL_LOAD_DATETM 
					,@SRC_FILE_DATE_JOB_APP AS SRC_FILE_DATE
					,@SRC_FILE_REC_CNT_JOB_APP AS SRC_TBL_REC_CNT
					) SRC
				ON TGT.TARGET_TBL_NAME= SRC.TARGET_TBL_NAME
				AND TGT.SRC_TBL_NAME  = SRC.SRC_TBL_NAME
				AND TGT.SRC_FILE_DATE = SRC.SRC_FILE_DATE
				WHEN MATCHED 
				THEN UPDATE
				SET -- Update the records in dest if matched
					 TGT.SRC_TBL_REC_CNT			= TGT.SRC_TBL_REC_CNT + SRC.SRC_TBL_REC_CNT
					,TGT.SRC_TBL_LOAD_DATETM		= SRC.SRC_TBL_LOAD_DATETM
                    ,TGT.TARGET_TBL_LOAD_DATETM     = GETDATE()	
					,TGT.ETL_UPDATE_DATETM			= GETDATE()					 
					,TGT.ETL_UPDATE_EMP_LOGIN_NAME  = SUSER_SNAME()	
					,TGT.REPROCESS_FLAG = 0			
				WHEN NOT MATCHED BY TARGET THEN
				INSERT 
				(		
					 TARGET_TBL_NAME
					,TARGET_TBL_LOAD_DATETM
					,SRC_TBL_NAME
					,SRC_TBL_LOAD_DATETM
					,SRC_FILE_DATE
					,SRC_TBL_REC_CNT
				)
				VALUES
				(
					 @TARGET_TBL_NAME
					,@CurrentTime
					,@SRC_TBL_NAME_JOB_APP
					,@SRC_CREATE_DATETM_JOB_APP
					,@SRC_FILE_DATE_JOB_APP
					,@SRC_FILE_REC_CNT_JOB_APP
				);
				SET @EVENTTIME = GETDATE()
                EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                                    ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                                    ,@EventName = 'END'
                                                    ,@EVENT_SQL_STATEMENT = 'INSERT INTO [ETL].[ODS_ETL_CTRL_TBL] ([TARGET_TBL_NAME],[TARGET_TBL_LOAD_DATETM],[SRC_TBL_NAME],[SRC_TBL_LOAD_DATETM],[SRC_FILE_DATE],[SRC_TBL_REC_CNT]) VALUES (@TARGET_TBL_NAME,@CurrentTime,@SRC_TBL_NAME,@SRC_CREATE_DATETM,@SRC_FILE_DATE,@SRC_FILE_REC_CNT)'
                                                    ,@Message = 'SUCCEDDED: LOGGING THE RECORD INTO "[ETL].[ODS_ETL_CTRL_TBL] " TABLE '
                                                    ,@EVENT_TIME = @EVENTTIME
                                                    ,@RecordsAffected = 0
                                                    ,@RecordsInserted = 0
                                                    ,@RecordsUpdated  = 0		
		COMMIT TRAN ;
     END TRY
     BEGIN CATCH --- if something errorred then print out error and rollback transaction, and quit sproc with an error
        SELECT
			    @ErrorNum = ERROR_NUMBER()
			    ,@ErrorText = ERROR_MESSAGE()
		IF (@@trancount > 0)
			ROLLBACK TRANSACTION
		SELECT	@msg = '@@Error in Proc ' + @spname + ': ' + ISNULL(CONVERT(VARCHAR,@ErrorNum),0) + ' ' + @ErrorText
		SELECT	@msg = 'FAILED: @@Error in Proc ' + @spname + ': ' + ISNULL(CONVERT(VARCHAR,@ErrorNum),0) + ' ' + @ErrorText
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'ERROR'
			                                ,@EVENT_SQL_STATEMENT = ' MERGE  PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP AS DEST 
																			 USING #DISPOSTN_REASN_TYP AS SRC 
																			 ON UPPER(DEST.DISPOSTN_REASN_TYP_NAME)	= UPPER(SRC.DISPOSTN_REASN_TYP_NAME)  '
			                                ,@Message = @msg
                                            ,@EVENT_TIME = @EVENTTIME
			                                ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		RAISERROR (@msg,16,127) WITH NOWAIT
		RETURN -1;
     END CATCH
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = ''
                                            ,@Message = ''
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = @MergeRecordCount
                                            ,@RecordsInserted = @RecordsInserted
                                            ,@RecordsUpdated  = @RecordsUpdated
    END TRY
	BEGIN CATCH
		SELECT 	@ErrorNum  = ERROR_NUMBER()
			   ,@ErrorText = ERROR_MESSAGE()
		IF (@@trancount > 0)
			ROLLBACK TRANSACTION
		SELECT	@msg = 'FAILED: @@Error in Proc ' + @spname + ': ' + ISNULL(CONVERT(VARCHAR,@ErrorNum),0) + ' ' + @ErrorText
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
											,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
											,@EventName = 'FAILED'
											,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT'
											,@Message = @msg
											,@EVENT_TIME = @EVENTTIME
											,@RecordsAffected = 0
											,@RecordsInserted = 0
											,@RecordsUpdated  = 0
		RAISERROR (@msg,16,127) WITH NOWAIT
		RETURN -1;
	END CATCH
	FINISH: 
	--SET @MergeRecordCount= @RecordsInserted+@RecordsUpdated
	--SELECT @MergeRecordCount AS MergeRecordCount,@RecordsInserted AS RecordsInserted,@RecordsUpdated AS RecordsUpdated
	IF OBJECT_ID ('tempdb..#DISPOSTN_REASN_TYP') IS NOT NULL
	DROP TABLE #DISPOSTN_REASN_TYP;
	
END

