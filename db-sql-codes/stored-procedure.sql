-- Stored procedure to update electronics watermark

CREATE PROCEDURE UpdateElectronicsWatermarkTable
    @lastload VARCHAR(200)
AS 
BEGIN 
    -- Start the transaction 
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Update the incremental column in the table 
        UPDATE electronics_watermark_table
        SET last_load = @lastload;
        
        -- Commit the transaction
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback on error
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END;
