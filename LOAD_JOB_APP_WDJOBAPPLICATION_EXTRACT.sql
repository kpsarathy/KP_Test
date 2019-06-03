USE PEOPLE_ODS
GO

IF OBJECT_ID ('dbo.LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT') IS NOT NULL
DROP PROCEDURE [dbo].[LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT]
GO


CREATE PROCEDURE [dbo].[LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT]
AS
BEGIN
----------------------------------------------------------------------------------------------------------------------------------------------------
--  Name              : LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT
--  Purpose           : 1. Synchronize between 
--                         JOB_APP and WDJobApplicationExtract
--                      2. It Synchronize only current month (SRC_FILE_Date) records from HrdwStaging..WDJobApplicationExtract. All the source views has filter to get current SRC_FILE_DATE
--							records.
--	Sample Call       : EXEC  [PEOPLE_ODS].[dbo].[LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT]					
--
--  Input Parameters  : NONE
--
--  Output Paramaters : NONE
--
--  Version           : 1.0
--  Date              : 04/10/2019
--  Author            : nsridharan
--   
----------------------------------------------------------------------------------------------------------------------------------------------------
--
-- Modification History
--
-- ModIfication : WOR-23423: Dev: Populate Lookup Domain ID fields in PEOPLE_ODS.DBO.JOB_APP 
--					AND also referring the additional lookup table(s) 
--					1.JOB_APP.JOB_OFFR_STAT_TYP_ID 
--					2.JOB_APP.RELOC_POLICY_TYP_ID 
--					3.JOB_APP.JOB_OFFR_DECLN_REASN_TYP_ID 
--					4.JOB_APP.JOB_APP_SRC_REF_TYP_ID  
--					5.JOB_APP.JOB_APP_SUB_SRC_REF_TYP_ID 
--					6.JOB_APP.JOB_APP_SRC_REFRL_TYP_ID
--					7.JOB_APP.DISPOSTN_REASN_TYP_ID 
-- Version      : 2.0								
-- Date         : 04/29/2019
-- Author       : hkathi
--
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
	DECLARE  @TARGET_TBL_NAME		    VARCHAR(250) = 'JOB_APP'
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

	SELECT @JOB_APP_SRC_SYS_ID=TGSR.SRC_SYS_ID
			,@JOB_APP_SUB_SRC_SYS_ID=TGSR.SUB_SRC_SYS_ID
	FROM PEOPLE_ODS.dbo.SRC_SYS_SUB_SRC_SYS  TGSR WITH (NOLOCK)
	LEFT JOIN PEOPLE_ODS.dbo.SRC_SYS SRC WITH (NOLOCK)
		ON TGSR.SRC_SYS_ID=SRC.SRC_SYS_ID
	JOIN PEOPLE_ODS.dbo.SUB_SRC_SYS TGT WITH (NOLOCK) 
		ON TGSR.SUB_SRC_SYS_ID=TGT.SUB_SRC_SYS_ID
	WHERE SRC_SYS_NAME=@SRC_SYS_NAME
	AND SUB_SRC_SYS_NAME = @SUB_SRC_SYS_NAME

	DECLARE @MergeResult TABLE 
	(	MergeAction VARCHAR(150)
		,JOB_APP_ID				VARCHAR(510)
		,JOB_APP_SRC_SYS_ID		INT
		,JOB_APP_SUB_SRC_SYS_ID	INT
	)
	BEGIN TRY
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_OFFR_STAT_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_JOB_OFFR_STAT_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_OFFR_STAT_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_JOB_OFFR_STAT_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_OFFR_STAT_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_JOB_OFFR_STAT_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_OFFR_STAT_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_RELOC_POLICY_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_RELOC_POLICY_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.RELOC_POLICY_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_RELOC_POLICY_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_RELOC_POLICY_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_RELOC_POLICY_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.RELOC_POLICY_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_DISPOSTN_REASN_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.DISPOSTN_REASN_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_JOB_APP_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SRC_REF_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_JOB_APP_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_JOB_APP_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SRC_REF_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SUB_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_JOB_APP_SUB_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SUB_SRC_REF_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_JOB_APP_SUB_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SUB_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_JOB_APP_SUB_SRC_REF_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SUB_SRC_REF_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SRC_REFRL_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_JOB_APP_SRC_REFRL_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SRC_REFRL_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_JOB_APP_SRC_REFRL_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_SRC_REFRL_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_JOB_APP_SRC_REFRL_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_APP_SRC_REFRL_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'START'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_OFFR_DECLN_REASN_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'STARTED: EXECUTING THE  SP (LOAD_JOB_OFFR_DECLN_REASN_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_OFFR_DECLN_REASN_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
        EXEC dbo.LOAD_JOB_OFFR_DECLN_REASN_TYP_WDJOBAPPLICATION_EXTRACT
        SET @EVENTTIME = GETDATE()
        EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                            ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                            ,@EventName = 'END'
                                            ,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_OFFR_DECLN_REASN_TYP_WDJOBAPPLICATION_EXTRACT'
                                            ,@Message = 'SUCCEEDED: EXECUTING THE  SP (LOAD_JOB_OFFR_DECLN_REASN_TYP_WDJOBAPPLICATION_EXTRACT) TO LOAD THE DATA INTO PEOPLE_ODS.DBO.JOB_OFFR_DECLN_REASN_TYP'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
											,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
											,@EventName='START'
											,@EVENT_SQL_STATEMENT='SELECT * INTO  #WDJOBAPPLICATIONEXTRACT FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
											,@Message='LOADING DATA INTO SESSION TABLE WITH ALL DATA TYPE CONVERSIONS IS STARTED'
											,@EVENT_TIME=@EVENTTIME
											,@RecordsAffected = 0
											,@RecordsInserted = 0
											,@RecordsUpdated  = 0
		IF OBJECT_ID ('tempdb..#WDJOBAPPLICATIONEXTRACT') IS NOT NULL
		DROP TABLE #WDJOBAPPLICATIONEXTRACT;	
	

		SELECT	 CAST(dbo.TrimBlankNull(ApplicationID) AS NVARCHAR(510))					                AS JOB_APP_ID  
			,@JOB_APP_SRC_SYS_ID											                                AS JOB_APP_SRC_SYS_ID
			,@JOB_APP_SUB_SRC_SYS_ID										                                AS JOB_APP_SUB_SRC_SYS_ID
            ,CAST(dbo.TrimBlankNull(CandidateID) AS NVARCHAR(510))		                                    AS CAND_ID
            ,CAST(dbo.TrimBlankNull(ReqID) AS NVARCHAR(100))		                                        AS JOB_REQN_ID
            ,CAST(dbo.TrimBlankNull(JobProfileID) AS NVARCHAR(50))		                                    AS JOB_CODE
            ,CAST(dbo.TrimBlankNull(CreatedDateTime) AS DATE)		                                        AS JOB_APP_CREATE_DATE
            ,CAST(dbo.TrimBlankNull(ModifiedDateTime) AS DATE)		                                        AS JOB_APP_UPDATE_DATE
            ,CAST(dbo.TrimBlankNull(OfferSentDate) AS DATE)		                                            AS JOB_OFFR_SENT_DATE
            ,CAST(dbo.TrimBlankNull(OfferExtendedDate) AS DATE)		                                        AS JOB_OFFR_EXTEND_DATE
            ,CAST(dbo.TrimBlankNull(OfferAcceptDate) AS DATE)		                                        AS JOB_OFFR_ACCPT_DATE
            ,CAST(dbo.TrimBlankNull(OfferDeclineDate) AS DATE)		                                        AS JOB_OFFR_DECLN_DATE
            ,CAST(dbo.TrimBlankNull(DateRescinded) AS DATE)		                                            AS JOB_OFFR_RESCIND_DATE
            ,CAST(dbo.TrimBlankNull(ExpectedStartDate) AS DATE)		                                        AS EXPECT_START_DATE
            ,dbo.UDF_GET_EMP_ID(ReportsToManager)                                                           AS MGR_EMP_ID
            ,dbo.UDF_GET_EMP_ID(ReferredBy)                                                                 AS REFRL_EMP_ID
            ,JOST.JOB_OFFR_STAT_TYP_ID                                                                      AS JOB_OFFR_STAT_TYP_ID 
            ,RPT.RELOC_POLICY_TYP_ID                                                                        AS JOB_REQN_RELOC_POLICY_TYP_ID
            ,DRP.DISPOSTN_REASN_TYP_ID                                                                      AS JOB_APP_DISPOSTN_REASN_TYP_ID
            ,JSRT.JOB_APP_SRC_REF_TYP_ID                                                                    AS JOB_APP_SRC_REF_TYP_ID
            ,JSSRT.JOB_APP_SUB_SRC_REF_TYP_ID                                                               AS JOB_APP_SUB_SRC_REF_TYP_ID
            ,JSRFLT.JOB_APP_SRC_REFRL_TYP_ID                                                                AS JOB_APP_SRC_REFRL_TYP_ID
            ,JODR.JOB_OFFR_DECLN_REASN_TYP_ID									                            AS JOB_OFFR_DECLN_REASN_TYP_ID
            ,CAST(dbo.TrimBlankNull(SalaryCurrencyCode) AS CHAR(3))	                                        AS JOB_REQN_SAL_CURRN_CODE
            ,CAST(dbo.TrimBlankNull(OfferCurrency) AS CHAR(3))	                                            AS JOB_OFFR_SAL_CURRN_CODE
            ,CAST(CASE WHEN dbo.TrimBlankNull(EUWorkEligibility) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(EUWorkEligibility) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS EU_WRK_ELIG_FLAG
            ,CAST(CASE WHEN dbo.TrimBlankNull(ImmigrationRequired) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(ImmigrationRequired) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS IMMI_REQ_FLAG
            ,CAST(CASE WHEN dbo.TrimBlankNull(ReachedHiringManagerReviewFlag) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(ReachedHiringManagerReviewFlag) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS REACH_MGR_REVIEW_FLAG
            ,CAST(CASE WHEN dbo.TrimBlankNull(ReachedHiringManagerScreenFlag) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(ReachedHiringManagerScreenFlag) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS REACH_MGR_SCREEN_FLAG
            ,CAST(CASE WHEN dbo.TrimBlankNull(ReachedRecruiterScreenFlag) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(ReachedRecruiterScreenFlag) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS REACH_RECRUIT_SCREEN_FLAG
            ,CAST(dbo.TrimBlankNull(SrcAvenueValue) AS SMALLINT)		                                    AS REFRL_FLAG
            ,CAST(CASE WHEN dbo.TrimBlankNull(RelocationRequired) IN ('Yes', 'True') THEN 1 
			WHEN dbo.TrimBlankNull(RelocationRequired) IN ('No', 'False') THEN 0 
			ELSE NULL END AS SMALLINT)                                                                      AS RELOC_REQ_FLAG
            ,CAST(dbo.TrimBlankNull(OfferOptions) AS INTEGER)		                                        AS JOB_OFFR_STOCK_OPTN_CNT
            ,CAST(dbo.TrimBlankNull(OfferBase) AS DECIMAL(19,4))		                                    AS JOB_OFFR_BASE_AMT_LOCAL
            ,CAST(dbo.TrimBlankNull(OfferBonus) AS DECIMAL(19,4))		                                    AS JOB_OFFR_BONUS_AMT_LOCAL
            ,CAST(dbo.TrimBlankNull(OfferSigningBonus) AS DECIMAL(19,4))                                    AS JOB_OFFR_SIGN_BONUS_AMT_LOCAL
            ,CAST(TRY_CAST(dbo.TrimBlankNull(AllowanceAmount1) AS money) AS DECIMAL(19,4))                  AS ALW_1_AMT_LOCAL
            ,CAST(TRY_CAST(dbo.TrimBlankNull(AllowanceAmount2) AS money) AS DECIMAL(19,4))                  AS ALW_2_AMT_LOCAL
            ,CAST(TRY_CAST(dbo.TrimBlankNull(AllowanceAmount3) AS money) AS DECIMAL(19,4))                  AS ALW_3_AMT_LOCAL
		INTO #WDJOBAPPLICATIONEXTRACT
		FROM HRDWStaging.dbo.WDJobApplicationExtract TG WITH (NOLOCK)
		LEFT JOIN dbo.RELOC_POLICY_TYP RPT WITH (NOLOCK)
		ON RPT.RELOC_POLICY_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.RelocationPolicy),'')
		LEFT JOIN dbo.DISPOSTN_REASN_TYP DRP WITH (NOLOCK)
		ON DRP.DISPOSTN_REASN_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.DISPOSITIONREASON),'')
		LEFT JOIN dbo.JOB_APP_SRC_REF_TYP JSRT WITH (NOLOCK)
		ON JSRT.JOB_APP_SRC_REF_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.SourceReferenceType),'')
		LEFT JOIN dbo.JOB_APP_SUB_SRC_REF_TYP JSSRT WITH (NOLOCK)
		ON JSSRT.JOB_APP_SUB_SRC_REF_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.SourceReferringValue),'')
		LEFT JOIN dbo.JOB_APP_SRC_REFRL_TYP JSRFLT WITH (NOLOCK)
		ON JSRFLT.JOB_APP_SRC_REFRL_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.SrcAvenueType),'')
		LEFT JOIN dbo.JOB_OFFR_DECLN_REASN_TYP JODR WITH (NOLOCK)
		ON JODR.JOB_OFFR_DECLN_REASN_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.OfferDeclineReason),'')
		LEFT JOIN dbo.JOB_OFFR_STAT_TYP JOST WITH (NOLOCK)
		ON JOST.JOB_OFFR_STAT_TYP_NAME = ISNULL(dbo.TrimBlankNull(TG.RARSTATUS),'')
		WHERE SourceExtractedDate = @SRC_FILE_DATE_JOB_APP

		SELECT @RCNT= @@ROWCOUNT
		SET @EVENTTIME = GETDATE()
		EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME=@spname 
											,@SRC_FILE_DATE=@SRC_FILE_DATE_JOB_APP
											,@EventName='END'
											,@EVENT_SQL_STATEMENT='SELECT * INTO  #WDJOBAPPLICATIONEXTRACT FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
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
                                            ,@EVENT_SQL_STATEMENT = 'SELECT * INTO  #WDJOBAPPLICATIONEXTRACT FROM HRDWStaging.dbo.WDJobApplicationExtract WITH (NOLOCK)'
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
                                            ,@EVENT_SQL_STATEMENT = 'MERGE PEOPLE_ODS.DBO.JOB_APP AS DEST 
	                                                                    USING #WDJOBAPPLICATIONEXTRACT AS SRC 
	                                                                    ON	DEST.JOB_APP_ID				= SRC.JOB_APP_ID
																		AND DEST.JOB_APP_SRC_SYS_ID		= SRC.JOB_APP_SRC_SYS_ID
																		AND DEST.JOB_APP_SUB_SRC_SYS_ID	= SRC.JOB_APP_SUB_SRC_SYS_ID  '
                                            ,@Message='STARTED: LOADING DATA INTO "PEOPLE_ODS.DBO.JOB_APP" TABLE WITH INSERT/UPDATE'
                                            ,@EVENT_TIME = @EVENTTIME
                                            ,@RecordsAffected = 0
                                            ,@RecordsInserted = 0
                                            ,@RecordsUpdated  = 0
		BEGIN TRAN
			   MERGE PEOPLE_ODS.DBO.JOB_APP AS DEST 
	           USING #WDJOBAPPLICATIONEXTRACT AS SRC 
	                ON	DEST.JOB_APP_ID					= SRC.JOB_APP_ID
		                AND DEST.JOB_APP_SRC_SYS_ID		= SRC.JOB_APP_SRC_SYS_ID
		                AND DEST.JOB_APP_SUB_SRC_SYS_ID	= SRC.JOB_APP_SUB_SRC_SYS_ID
                WHEN MATCHED 
                AND 
                ( 
							ISNULL(DEST.[CAND_ID],'')					            <>	ISNULL(SRC.[CAND_ID],'')					
						OR	ISNULL(DEST.[JOB_REQN_ID],'')							<> 	ISNULL(SRC.[JOB_REQN_ID],'')								
						OR	ISNULL(DEST.[JOB_CODE],'')							    <> 	ISNULL(SRC.[JOB_CODE],'')
                        OR	ISNULL(DEST.[JOB_APP_CREATE_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[JOB_APP_CREATE_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_APP_UPDATE_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[JOB_APP_UPDATE_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_OFFR_SENT_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[JOB_OFFR_SENT_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_OFFR_EXTEND_DATE],'1900-01-01')		<> 	ISNULL(SRC.[JOB_OFFR_EXTEND_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_OFFR_ACCPT_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[JOB_OFFR_ACCPT_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_OFFR_DECLN_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[JOB_OFFR_DECLN_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[JOB_OFFR_RESCIND_DATE],'1900-01-01')		<> 	ISNULL(SRC.[JOB_OFFR_RESCIND_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[EXPECT_START_DATE],'1900-01-01')		    <> 	ISNULL(SRC.[EXPECT_START_DATE],'1900-01-01')
                        OR	ISNULL(DEST.[MGR_EMP_ID],'')				            <> 	ISNULL(SRC.[MGR_EMP_ID],'')
                        OR	ISNULL(DEST.[REFRL_EMP_ID],'')				            <> 	ISNULL(SRC.[REFRL_EMP_ID],'')		
                        OR	ISNULL(DEST.[JOB_OFFR_STAT_TYP_ID],0)					<> 	ISNULL(SRC.[JOB_OFFR_STAT_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_REQN_RELOC_POLICY_TYP_ID],0)			<> 	ISNULL(SRC.[JOB_REQN_RELOC_POLICY_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_APP_DISPOSTN_REASN_TYP_ID],0)			<> 	ISNULL(SRC.[JOB_APP_DISPOSTN_REASN_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_APP_SRC_REF_TYP_ID],0)					<> 	ISNULL(SRC.[JOB_APP_SRC_REF_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_APP_SUB_SRC_REF_TYP_ID],0)				<> 	ISNULL(SRC.[JOB_APP_SUB_SRC_REF_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_APP_SRC_REFRL_TYP_ID],0)				<> 	ISNULL(SRC.[JOB_APP_SRC_REFRL_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_OFFR_DECLN_REASN_TYP_ID],0)			<> 	ISNULL(SRC.[JOB_OFFR_DECLN_REASN_TYP_ID],0)
                        OR	ISNULL(DEST.[JOB_REQN_SAL_CURRN_CODE],'')				<> 	ISNULL(SRC.[JOB_REQN_SAL_CURRN_CODE],'')
                        OR	ISNULL(DEST.[JOB_OFFR_SAL_CURRN_CODE],'')				<> 	ISNULL(SRC.[JOB_OFFR_SAL_CURRN_CODE],'')
                        OR	ISNULL(DEST.[EU_WRK_ELIG_FLAG],0)					    <> 	ISNULL(SRC.[EU_WRK_ELIG_FLAG],0)
                        OR	ISNULL(DEST.[IMMI_REQ_FLAG],0)					        <> 	ISNULL(SRC.[IMMI_REQ_FLAG],0)
                        OR	ISNULL(DEST.[REACH_MGR_REVIEW_FLAG],0)					<> 	ISNULL(SRC.[REACH_MGR_REVIEW_FLAG],0)
                        OR	ISNULL(DEST.[REACH_MGR_SCREEN_FLAG],0)					<> 	ISNULL(SRC.[REACH_MGR_SCREEN_FLAG],0)
                        OR	ISNULL(DEST.[REACH_RECRUIT_SCREEN_FLAG],0)				<> 	ISNULL(SRC.[REACH_RECRUIT_SCREEN_FLAG],0)
                        OR	ISNULL(DEST.[REFRL_FLAG],0)					            <> 	ISNULL(SRC.[REFRL_FLAG],0)
                        OR	ISNULL(DEST.[RELOC_REQ_FLAG],0)					        <> 	ISNULL(SRC.[RELOC_REQ_FLAG],0)
                        OR	ISNULL(DEST.[JOB_OFFR_STOCK_OPTN_CNT],0)				<> 	ISNULL(SRC.[JOB_OFFR_STOCK_OPTN_CNT],0)
                        OR	ISNULL(DEST.[JOB_OFFR_BASE_AMT_LOCAL],0)				<> 	ISNULL(SRC.[JOB_OFFR_BASE_AMT_LOCAL],0)
                        OR	ISNULL(DEST.[JOB_OFFR_BONUS_AMT_LOCAL],0)			    <> 	ISNULL(SRC.[JOB_OFFR_BONUS_AMT_LOCAL],0)
                        OR	ISNULL(DEST.[JOB_OFFR_SIGN_BONUS_AMT_LOCAL],0)		    <> 	ISNULL(SRC.[JOB_OFFR_SIGN_BONUS_AMT_LOCAL],0)
                        OR	ISNULL(DEST.[ALW_1_AMT_LOCAL],0)				        <> 	ISNULL(SRC.[ALW_1_AMT_LOCAL],0)
                        OR	ISNULL(DEST.[ALW_2_AMT_LOCAL],0)				        <> 	ISNULL(SRC.[ALW_2_AMT_LOCAL],0)
                        OR	ISNULL(DEST.[ALW_3_AMT_LOCAL],0)				        <> 	ISNULL(SRC.[ALW_3_AMT_LOCAL],0)
                )
                THEN UPDATE
	            SET -- Update the records in dest if matched
		             DEST.[CAND_ID]					            =	SRC.[CAND_ID]					
					,DEST.[JOB_REQN_ID]							= 	SRC.[JOB_REQN_ID]								
					,DEST.[JOB_CODE]						    =   SRC.[JOB_CODE]
                    ,DEST.[JOB_APP_CREATE_DATE]	                =   SRC.[JOB_APP_CREATE_DATE]
                    ,DEST.[JOB_APP_UPDATE_DATE]	                =   SRC.[JOB_APP_UPDATE_DATE]
                    ,DEST.[JOB_OFFR_SENT_DATE]	                =   SRC.[JOB_OFFR_SENT_DATE]
                    ,DEST.[JOB_OFFR_EXTEND_DATE]	            =   SRC.[JOB_OFFR_EXTEND_DATE]
                    ,DEST.[JOB_OFFR_ACCPT_DATE]	                =   SRC.[JOB_OFFR_ACCPT_DATE]
                    ,DEST.[JOB_OFFR_DECLN_DATE]	                =   SRC.[JOB_OFFR_DECLN_DATE]
                    ,DEST.[JOB_OFFR_RESCIND_DATE]	            =   SRC.[JOB_OFFR_RESCIND_DATE]
                    ,DEST.[EXPECT_START_DATE]		            =   SRC.[EXPECT_START_DATE]
                    ,DEST.[MGR_EMP_ID]				            =   SRC.[MGR_EMP_ID]
                    ,DEST.[REFRL_EMP_ID]				        =   SRC.[REFRL_EMP_ID]		
                    ,DEST.[JOB_OFFR_STAT_TYP_ID]				=   SRC.[JOB_OFFR_STAT_TYP_ID]
                    ,DEST.[JOB_REQN_RELOC_POLICY_TYP_ID]		=   SRC.[JOB_REQN_RELOC_POLICY_TYP_ID]
                    ,DEST.[JOB_APP_DISPOSTN_REASN_TYP_ID]	    =   SRC.[JOB_APP_DISPOSTN_REASN_TYP_ID]
                    ,DEST.[JOB_APP_SRC_REF_TYP_ID]			    =   SRC.[JOB_APP_SRC_REF_TYP_ID]
                    ,DEST.[JOB_APP_SUB_SRC_REF_TYP_ID]		    =   SRC.[JOB_APP_SUB_SRC_REF_TYP_ID]
                    ,DEST.[JOB_APP_SRC_REFRL_TYP_ID]			=   SRC.[JOB_APP_SRC_REFRL_TYP_ID]
                    ,DEST.[JOB_OFFR_DECLN_REASN_TYP_ID]		    =   SRC.[JOB_OFFR_DECLN_REASN_TYP_ID]
                    ,DEST.[JOB_REQN_SAL_CURRN_CODE]			    =   SRC.[JOB_REQN_SAL_CURRN_CODE]
                    ,DEST.[JOB_OFFR_SAL_CURRN_CODE]			    =   SRC.[JOB_OFFR_SAL_CURRN_CODE]
                    ,DEST.[EU_WRK_ELIG_FLAG]					=   SRC.[EU_WRK_ELIG_FLAG]
                    ,DEST.[IMMI_REQ_FLAG]					    =   SRC.[IMMI_REQ_FLAG]
                    ,DEST.[REACH_MGR_REVIEW_FLAG]			    =   SRC.[REACH_MGR_REVIEW_FLAG]
                    ,DEST.[REACH_MGR_SCREEN_FLAG]			    =   SRC.[REACH_MGR_SCREEN_FLAG]
                    ,DEST.[REACH_RECRUIT_SCREEN_FLAG]		    =   SRC.[REACH_RECRUIT_SCREEN_FLAG]
                    ,DEST.[REFRL_FLAG]					        =   SRC.[REFRL_FLAG]
                    ,DEST.[RELOC_REQ_FLAG]					    =   SRC.[RELOC_REQ_FLAG]
                    ,DEST.[JOB_OFFR_STOCK_OPTN_CNT]			    =   SRC.[JOB_OFFR_STOCK_OPTN_CNT]
                    ,DEST.[JOB_OFFR_BASE_AMT_LOCAL]			    =   SRC.[JOB_OFFR_BASE_AMT_LOCAL]
                    ,DEST.[JOB_OFFR_BONUS_AMT_LOCAL]			=   SRC.[JOB_OFFR_BONUS_AMT_LOCAL]
                    ,DEST.[JOB_OFFR_SIGN_BONUS_AMT_LOCAL]	    =   SRC.[JOB_OFFR_SIGN_BONUS_AMT_LOCAL]
                    ,DEST.[ALW_1_AMT_LOCAL]				        =   SRC.[ALW_1_AMT_LOCAL]
                    ,DEST.[ALW_2_AMT_LOCAL]				        =   SRC.[ALW_2_AMT_LOCAL]
                    ,DEST.[ALW_3_AMT_LOCAL]				        =   SRC.[ALW_3_AMT_LOCAL]										
		            ,DEST.ETL_CREATE_DATETM						=	@ETL_CREATE_DATETM			 	
		            ,DEST.ETL_CREATE_EMP_LOGIN_NAME				=	@ETL_CREATE_EMP_LOGIN_NAME		
		            ,DEST.ETL_UPDATE_DATETM						=	@ETL_UPDATE_DATETM				
		            ,DEST.ETL_UPDATE_EMP_LOGIN_NAME				=	@ETL_UPDATE_EMP_LOGIN_NAME		
                WHEN NOT MATCHED BY TARGET THEN
                INSERT 
                (
					   JOB_APP_ID
					  ,JOB_APP_SRC_SYS_ID
					  ,JOB_APP_SUB_SRC_SYS_ID
                      ,CAND_ID
                      ,JOB_REQN_ID
                      ,JOB_CODE
                      ,JOB_APP_CREATE_DATE
                      ,JOB_APP_UPDATE_DATE
                      ,JOB_OFFR_SENT_DATE
                      ,JOB_OFFR_EXTEND_DATE
                      ,JOB_OFFR_ACCPT_DATE
                      ,JOB_OFFR_DECLN_DATE
                      ,JOB_OFFR_RESCIND_DATE
                      ,EXPECT_START_DATE
                      ,MGR_EMP_ID
                      ,REFRL_EMP_ID
                      ,JOB_OFFR_STAT_TYP_ID
                      ,JOB_REQN_RELOC_POLICY_TYP_ID
                      ,JOB_APP_DISPOSTN_REASN_TYP_ID
                      ,JOB_APP_SRC_REF_TYP_ID
                      ,JOB_APP_SUB_SRC_REF_TYP_ID
                      ,JOB_APP_SRC_REFRL_TYP_ID
                      ,JOB_OFFR_DECLN_REASN_TYP_ID
                      ,JOB_REQN_SAL_CURRN_CODE
                      ,JOB_OFFR_SAL_CURRN_CODE
                      ,EU_WRK_ELIG_FLAG
                      ,IMMI_REQ_FLAG
                      ,REACH_MGR_REVIEW_FLAG
                      ,REACH_MGR_SCREEN_FLAG
                      ,REACH_RECRUIT_SCREEN_FLAG
                      ,REFRL_FLAG
                      ,RELOC_REQ_FLAG
                      ,JOB_OFFR_STOCK_OPTN_CNT
                      ,JOB_OFFR_BASE_AMT_LOCAL
                      ,JOB_OFFR_BONUS_AMT_LOCAL
                      ,JOB_OFFR_SIGN_BONUS_AMT_LOCAL
                      ,ALW_1_AMT_LOCAL
                      ,ALW_2_AMT_LOCAL
                      ,ALW_3_AMT_LOCAL
					  ,ETL_CREATE_DATETM
					  ,ETL_CREATE_EMP_LOGIN_NAME
					  ,ETL_UPDATE_DATETM
					  ,ETL_UPDATE_EMP_LOGIN_NAME
                )
                VALUES 
                (
					   SRC.JOB_APP_ID
					  ,SRC.JOB_APP_SRC_SYS_ID
					  ,SRC.JOB_APP_SUB_SRC_SYS_ID					     
                      ,SRC.CAND_ID
                      ,SRC.JOB_REQN_ID
                      ,SRC.JOB_CODE
                      ,SRC.JOB_APP_CREATE_DATE
                      ,SRC.JOB_APP_UPDATE_DATE
                      ,SRC.JOB_OFFR_SENT_DATE
                      ,SRC.JOB_OFFR_EXTEND_DATE
                      ,SRC.JOB_OFFR_ACCPT_DATE
                      ,SRC.JOB_OFFR_DECLN_DATE
                      ,SRC.JOB_OFFR_RESCIND_DATE
                      ,SRC.EXPECT_START_DATE
                      ,SRC.MGR_EMP_ID
                      ,SRC.REFRL_EMP_ID
                      ,SRC.JOB_OFFR_STAT_TYP_ID
                      ,SRC.JOB_REQN_RELOC_POLICY_TYP_ID
                      ,SRC.JOB_APP_DISPOSTN_REASN_TYP_ID
                      ,SRC.JOB_APP_SRC_REF_TYP_ID
                      ,SRC.JOB_APP_SUB_SRC_REF_TYP_ID
                      ,SRC.JOB_APP_SRC_REFRL_TYP_ID
                      ,SRC.JOB_OFFR_DECLN_REASN_TYP_ID
                      ,SRC.JOB_REQN_SAL_CURRN_CODE
                      ,SRC.JOB_OFFR_SAL_CURRN_CODE
                      ,SRC.EU_WRK_ELIG_FLAG
                      ,SRC.IMMI_REQ_FLAG
                      ,SRC.REACH_MGR_REVIEW_FLAG
                      ,SRC.REACH_MGR_SCREEN_FLAG
                      ,SRC.REACH_RECRUIT_SCREEN_FLAG
                      ,SRC.REFRL_FLAG
                      ,SRC.RELOC_REQ_FLAG
                      ,SRC.JOB_OFFR_STOCK_OPTN_CNT
                      ,SRC.JOB_OFFR_BASE_AMT_LOCAL
                      ,SRC.JOB_OFFR_BONUS_AMT_LOCAL
                      ,SRC.JOB_OFFR_SIGN_BONUS_AMT_LOCAL
                      ,SRC.ALW_1_AMT_LOCAL
                      ,SRC.ALW_2_AMT_LOCAL
                      ,SRC.ALW_3_AMT_LOCAL
                      ,@ETL_CREATE_DATETM
                      ,@ETL_CREATE_EMP_LOGIN_NAME
                      ,@ETL_UPDATE_DATETM
                      ,@ETL_UPDATE_EMP_LOGIN_NAME
	            ) 
                OUTPUT $ACTION AS MergeAction,	
			                 SRC.JOB_APP_ID
			                ,SRC.JOB_APP_SRC_SYS_ID
			                ,SRC.JOB_APP_SUB_SRC_SYS_ID
                            INTO @MergeResult; 

				SELECT @RecordsInserted = @RecordsInserted + COUNT(*) FROM @MergeResult WHERE MergeAction = 'INSERT';
                SELECT @RecordsUpdated = @RecordsUpdated + COUNT(*)FROM @MergeResult WHERE MergeAction = 'UPDATE';
                SET @MergeRecordCount = @RecordsInserted + @RecordsUpdated
				SET @EVENTTIME = GETDATE()
                EXEC [ETL].[WRITE_ETL_EXECUTION_LOG] @TASK_NAME = @spname 
                                                    ,@SRC_FILE_DATE = @SRC_FILE_DATE_JOB_APP
                                                    ,@EventName = 'END'
                                                    ,@EVENT_SQL_STATEMENT = 'MERGE PEOPLE_ODS.DBO.JOB_APP AS DEST 
	                                                                         USING #WDJOBAPPLICATIONEXTRACT AS SRC 
	                                                                         ON	DEST.JOB_APP_ID				= SRC.JOB_APP_ID
																		 AND DEST.JOB_APP_SRC_SYS_ID		= SRC.JOB_APP_SRC_SYS_ID
																		 AND DEST.JOB_APP_SUB_SRC_SYS_ID	= SRC.JOB_APP_SUB_SRC_SYS_ID   '
                                                    ,@Message = 'SUCCEDED: LOADING DATA INTO "PEOPLE_ODS.DBO.JOB_APP" TABLE WITH INSERT/UPDATE'
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
			                                ,@EVENT_SQL_STATEMENT = 'MERGE PEOPLE_ODS.DBO.JOB_APP AS DEST 
				                                                        USING #WDJOBAPPLICATIONEXTRACT AS SRC 
				                                                        ON	DEST.JOB_APP_ID				= SRC.JOB_APP_ID
																		AND DEST.JOB_APP_SRC_SYS_ID		= SRC.JOB_APP_SRC_SYS_ID
																		AND DEST.JOB_APP_SUB_SRC_SYS_ID	= SRC.JOB_APP_SUB_SRC_SYS_ID  '
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
											,@EVENT_SQL_STATEMENT = 'EXEC dbo.LOAD_JOB_APP_WDJOBAPPLICATION_EXTRACT'
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
	IF OBJECT_ID ('tempdb..#WDJOBAPPLICATIONEXTRACT') IS NOT NULL
	DROP TABLE #WDJOBAPPLICATIONEXTRACT;
	
END

