Imports MySql.Data.MySqlClient
Imports EaTools.Ea
Imports EaTools.DataTools

Partial Public Class MySqlTools

#Region " Class Constants, ENums and Variables "

#Region " Constants "

    Public Const NonLinkedAdded As String = "NL"
    Public Const NonLinkedTableName As String = "NonLinked"

#End Region

#Region " Variables "

    Private Shared _fixedMessage As String = String.Empty

    Private Shared FixingNonLinkedRows As Boolean = False

#End Region

#End Region

#Region " Properties "

    Public Shared ReadOnly Property FixedMessage As String
        Get
            Return _fixedMessage
        End Get
    End Property

#End Region

#Region " Fix Concurrency Exception "

    Public Shared Function FixConcurrencyException(MySqlConn As MySqlConnection,
                                                   rowWithConcurEx As DataRow,
                                                   rurRowInDb As DataRow,
                                                   tempTable As DataTable) As FixedConcurExTypes

        ' tries to fix Concurrency error based on value in UpdatedWhen column
        '
        ' vars passed:
        '   MySqlConn - mySql connection
        '   rowWithErr - row with error
        '   curRowInDb - current row in database
        '   tempTable - temp table used when fixing concurrency error
        '
        ' returns:
        '   FixedConcurExTypes.NeedToUpdate - fixed, need to re-call update(table, false)
        '   FixedConcurExTypes.NeedToRefresh - fixed, need to refresh table
        '   FixedConcurExTypes.CouldNotFix - could not fixed
        '     - already fixing table
        '     - added wrong # of rows
        '     - error inserting row
        '     - error keeping row
        '     - something went wrong

        Try
            If Not tempTable.DataSet Is Nothing Then                                                        ' if temp table already in a dataset
                _errorMessage = String.Format("Table ""{0}"" is currently fixing another Concurrency error.", tempTable.TableName)
                _errorCode = teAlreadyFixing
                Return MySqlTools.FixedConcurExTypes.CouldNotFix
            End If

            Using tempDS As New DataSet
                tempDS.Tables.Add(tempTable)                                                                ' add temp table to temp data set
                Try
                    Dim FixedType As FixedConcurExTypes
                    If rurRowInDb Is Nothing Then                                                           ' if other user deleted row
                        Dim rowCount As Integer
                        If rowWithConcurEx.RowState = DataRowState.Deleted Then                             ' if RowWithConcurEx was deleted
                            Return FixedConcurExTypes.NeedToRefresh
                        Else                                                                                ' else other user delete row
                            Dim origRow As DataRow = GetDataRow(rowWithConcurEx, DataRowVersion.Original)   ' get original row
                            rowCount = AddRowViaSql(MySqlConn, rowWithConcurEx.Table.TableName, origRow)    ' add original row back to database
                            If rowCount = 0 OrElse rowCount > 1 Then                                        ' if did not ad 1 row
                                _errorMessage = String.Format("Wrong number ({0}) of rows inserted", rowCount)
                                _errorCode = teWrongRowCountErr
                                Return FixedConcurExTypes.CouldNotFix
                            ElseIf rowCount < 0 Then                                                        ' else if got an error
                                _errorMessage = "Error inserting row"
                                _errorCode = rowCount
                                Return FixedConcurExTypes.CouldNotFix
                            End If
                            tempTable.ImportRow(origRow)                                                    ' import orig row into temp table
                            FixedType = KeepRow(rowWithConcurEx, rowWithConcurEx.Table, tempTable)          ' keep row with concur ex
                            If FixedType = FixedConcurExTypes.CouldNotFix Then                              ' if error keeping row                                
                                ' errorMessage and _error code set in KeepRow()
                                Return FixedConcurExTypes.CouldNotFix
                            End If
                            Return FixedConcurExTypes.NeedToUpdate
                        End If
                    Else                                                                                    ' else user deleted or edited row                        
                        Dim RowUpdatedWhen As Date = GcrcvAs(rowWithConcurEx, tsUpdatedWhenColName, NoDate) ' get when row with concur ex updated
                        Dim CrUpdatedWhen As Date = GcrcvAs(rurRowInDb, tsUpdatedWhenColName, NoDate)       ' get when current row updated
                        If RowUpdatedWhen <> NoDate AndAlso CrUpdatedWhen <> NoDate Then                    ' if got an update for either row
                            If RowUpdatedWhen >= CrUpdatedWhen Then                                         ' if concur ex row has most recent data
                                FixedType = KeepRow(rowWithConcurEx, rowWithConcurEx.Table, tempTable)      ' keep row with concur ex
                                If FixedType = FixedConcurExTypes.CouldNotFix Then                          ' if error keeping row
                                    ' errorMessage and _error code set in KeepRow()
                                    Return FixedConcurExTypes.CouldNotFix
                                End If
                                Return FixedConcurExTypes.NeedToUpdate
                            Else                                                                            ' else current row has most recent data
                                FixedType = KeepRow(rurRowInDb, rowWithConcurEx.Table, tempTable)           ' keep current db row 
                                If FixedType = FixedConcurExTypes.CouldNotFix Then                          ' if error keeping row
                                    ' errorMessage and _error code set in KeepRow()
                                    Return FixedConcurExTypes.CouldNotFix
                                End If
                                Return FixedConcurExTypes.NeedToUpdate
                            End If
                        Else                                                                                ' else no updated when for either row
                            FixedType = KeepRow(rowWithConcurEx, rowWithConcurEx.Table, tempTable)          ' keep row with concur ex
                            If FixedType = FixedConcurExTypes.CouldNotFix Then                              ' if error keeping row
                                ' errorMessage and _error code set in KeepRow()
                                Return FixedConcurExTypes.CouldNotFix
                            End If
                            Return FixedConcurExTypes.NeedToUpdate
                        End If
                    End If

                    ' if got here the fixed error
                    'Return 1 ' one row's concurrency error fixed
                Catch ex As Exception
                    _errorMessage = String.Format("Fixing concurrency error for table ""{0}"".  {1}", tempTable.TableName, ex.Message)
                    _errorCode = teConcurrencyErr
                    Return FixedConcurExTypes.CouldNotFix
                End Try
            End Using
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return FixedConcurExTypes.CouldNotFix
        End Try
    End Function

    Public Shared Function GetCurrentRowInDb(MySqlConn As MySqlConnection, RowWithErr As DataRow, tempTable As DataTable) As DataRow

        ' tries to retrieve the current row from the database, based on the key field values in RowWithError
        '
        ' note: TempTable will be left with either 1 (RowWithError found) or 0 (RowWithError NOT found) rows
        '
        ' vars passed:
        '   MySqlConn - MySQL connection to user
        '   RowWithErr - row with error
        '   TempTable - temp table used when fixing concurrency error
        '   
        ' returns:
        '   DataRow - current matching row in the database
        '   Nothing -
        '       row's table not keyed
        '       row was not found in table in database
        '       more than 1 matching row was found in database
        '       some other error

        Const SelectSQL As String = "SELECT * FROM "

        Try
            If RowWithErr.Table.PrimaryKey.Length = 0 Then                                                              ' if table is not keyed
                Return Nothing                                                                                          ' return nothing
            End If

            ' SELECT * FROM tablename
            ' WHERE (ColumnName1 = Value1)

            Dim WhereSQL As String = GetWhereSqlForRow(RowWithErr)                                                      ' to select just row with error
            Dim SelectSQLText As String = String.Format("{0} {1} {2}", SelectSQL, RowWithErr.Table.TableName, WhereSQL) ' get full select command text
            ' create table adapter with just select command
            Dim tableAdapt As MySqlDataAdapter = CreateMySqlDataAdapt(MySqlConn, RowWithErr.Table.TableName, SelectSQLText, "", "", "")

            Dim rowCount As Integer = MySqlTools.FillTable(tempTable, tableAdapt)                                       ' fill just current values for rowWithErr
            If rowCount = 0 OrElse rowCount > 1 Then                                                                    ' if filled wrong # of rows
                _errorMessage = String.Format("Wrong row count ({0})", rowCount)
                _errorCode = teWrongRowCountErr
                Return Nothing                                                                                          ' return nothing
            ElseIf rowCount < 0 Then                                                                                    ' if got an error
                ' _errorMessage and _errorCode set in Fill                
                Return Nothing                                                                                          ' return error
            End If

            Return tempTable.Rows(0)                     ' current row is only row in temp table

            'Return GetCurrentRowInDb(RowWithErr, tempTable)                                                             ' return just current row in database
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return Nothing
        End Try
    End Function


    Private Shared Function KeepRow(rowToKeep As DataRow,
                                    table As DataTable,
                                    tempTable As DataTable) As FixedConcurExTypes

        ' enables keeping of a row when the row has been changed by another user
        ' for example
        '   1) user x fills table z
        '   2) user x makes changes to row 1
        '   3) user y fills table z
        '   4) user y makes changes to row 1
        '   5) user y updates row 1 in table z
        '   6) user x updates row 1 in table z (causes Concurrency Exception)
        '   this func will enable step 6 to complete
        '
        ' vars passed:
        '   rowToKeep - row values to be kept (can be current user's row or current row in database)
        '   TempTable - table with copy of current row data in database             
        '
        ' returns
        '   1 - 1 row updated
        '   0 - update did not work
        '   teNoPrimaryKey - no primary key for rowToKeep
        '   teRowNotFound - matching row not found in current database table 
        '   teOtherErr - something went wrong

        Try
            Dim KeyVals() As Object = GetRowKeyValues(rowToKeep)                            ' get key values
            If KeyVals Is Nothing Then                                                      ' if no key values                    
                _errorMessage = "No primary key"
                _errorCode = teNoPrimaryKey
                Return FixedConcurExTypes.CouldNotFix
            End If

            Dim t2Table As DataTable = rowToKeep.Table.Clone                                ' make a clone of the temp table
            t2Table.Rows.Add(rowToKeep.ItemArray)                                           ' add row to keep to temp table 2
            MergeTableIntoDataSet(tempTable, table.DataSet, PreserveChangesTypes.Yes)       ' merge current data
            Dim rowToRestore As DataRow = FindRow(table, KeyVals)                           ' find row to restore
            If rowToRestore Is Nothing Then                                                 ' if did not find row to restore
                _errorMessage = "Could not find row to restore"
                _errorCode = teRowNotFound
                Return FixedConcurExTypes.CouldNotFix
            End If
            CopyRow(t2Table.Rows(0), rowToRestore)                                          ' restore row values (user's edits) 
            Return FixedConcurExTypes.NeedToUpdate
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return FixedConcurExTypes.CouldNotFix
        End Try
    End Function

    Public Shared Sub MergeTableIntoDataSet(tempTable As DataTable,
                                            ds As DataSet,
                                            preserveChanges As PreserveChangesTypes)

        ' merges temp table into dataset
        '   
        ' vars passed:
        '   tempTable - table to merge
        '   ds - data set to merge into
        '   preserveChanges - preserve changes option

        Try
            Select Case preserveChanges
                Case PreserveChangesTypes.None
                    ds.Merge(tempTable)
                Case PreserveChangesTypes.No
                    ds.Merge(tempTable, False, MissingSchemaAction.Ignore)
                Case PreserveChangesTypes.Yes
                    ds.Merge(tempTable, True, MissingSchemaAction.Ignore)
            End Select
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
        End Try
    End Sub

#End Region

#Region " Fix Constraint Exception "

    Private Shared Function CheckNonLinkedTable(ChildTable As DataTable) As Integer

        ' checks the table with non linked rows, making sure it has:
        ' 1) there is a table
        ' 2) only 1 parent relation
        ' 3) parent table has rows
        ' 4) only 1 linked column in parent table
        ' 5) only 1 linked column in child table
        '
        ' vars passed:
        '   ChildTable - table containing non linked rows
        '   
        ' returns: 
        '   NoErrors - no errors
        '   teNoTableErr - no table
        '   nlNoParentRels - no parent relations
        '   nlMultiParentRels - multiple parent relations
        '   nlNoLinkedColumnsErr - no linked column in parent or child table
        '   nlMultiLinkedColumnsErr - multiple linked columns in parent ot child table
        '   teOtherErr - other error 

        Try
            Dim ChildTableName As String = ChildTable.TableName

            ' make sure got only 1 parent relation
            If ChildTable.ParentRelations.Count = 0 Then
                _errorMessage = String.Format("No Parent Relation for table ""{0}"".", ChildTable.TableName)
                _errorCode = nlNoParentRelsErr
                Return nlNoParentRelsErr
            End If
            If ChildTable.ParentRelations.Count > 1 Then
                _errorMessage = String.Format("Multiple Parent Relation for table ""{0}"".", ChildTable.TableName)
                _errorCode = nlMultiParentRelsErr
                Return nlMultiParentRelsErr
            End If
            ' get parent table name
            Dim ParentTableName As String = ChildTable.ParentRelations(0).ParentTable.TableName

            ' make sure only 1 parent column linked to only 1 child column
            If ChildTable.ParentRelations(0).ChildKeyConstraint.RelatedColumns.Count = 0 Then
                _errorMessage = String.Format("No Linked Column in parent table ""{0}"".", ParentTableName)
                _errorCode = nlNoLinkedColumnsErr
                Return nlNoLinkedColumnsErr
            End If
            If ChildTable.ParentRelations(0).ChildKeyConstraint.RelatedColumns.Count > 1 Then
                _errorMessage = String.Format("Multiple Linked Columns in parent table ""{0}"".", ParentTableName)
                _errorCode = nlNoLinkedColumnsErr
                Return nlMultiLinkedColumnsErr
            End If
            If ChildTable.ParentRelations(0).ChildKeyConstraint.Columns.Count = 0 Then
                EaTools.Tools.ShowErrorMessage(String.Format("No Linked Column in child table ""{0}"".", ChildTableName), etFixingConstraintErr)
                Return nlNoLinkedColumnsErr
            End If
            If ChildTable.ParentRelations(0).ChildKeyConstraint.Columns.Count > 1 Then
                _errorMessage = String.Format("Multiple Linked Columns in child table ""{0}"".", ChildTableName)
                Return nlMultiLinkedColumnsErr
                Return nlMultiLinkedColumnsErr
            End If
            Return NoErrors
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return teOtherErr
        End Try
    End Function

    Private Shared Function CreateNonLinked(MySqlConn As MySqlConnection,
                                            ChildTable As DataTable,
                                            Optional CheckChildTable As Boolean = True) As Integer

        ' created the table "NonLinked" from non linked rows in aTable
        '
        ' note:  this function assumes:
        '   1) only 1 parent relation, so can use zero index in aTable.ParentRelations
        '   2) only one column from the parent table is linked to one column in the child table
        '
        ' vars passed:
        '   MySqlConn - MySql connection
        '   ChildTable - table containing non linked rows
        '   CheckChildTable - if TRUE, then need to check child table
        '
        ' returns: 
        '   >= 0 # of rows inserted into NonLinked table
        '   teNoTableErr - no table
        '   nlNoParentRels - no parent relations
        '   nlMultiParentRels - multiple parent relations
        '   nlNoLinkedColumnsErr - no linked column in parent or child table
        '   nlMultiLinkedColumnsErr - multiple linked columns in parent ot child table
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlNoTableName - no table name
        '   sqlErr - other error in sql 
        '   sqlDropErr - other error in dropping table
        '   teOtherErr - other error 

        Try
            Dim ChildTableName As String = String.Empty
            Dim NlErr As Integer
            If CheckChildTable Then                         ' if need to check if child table is OK 
                NlErr = CheckNonLinkedTable(ChildTable)     ' check if table is ok to remove non linked rows
                If NlErr <> NoErrors Then                   ' if got error in check
                    Return NlErr                            ' return error
                End If
            End If

            If MySqlTools.TableExists(MySqlConn, NonLinkedTableName) = MySqlTools.ExistsTypes.Yes Then          ' if got temp NonLinked table
                NlErr = MySqlTools.DropTable(MySqlConn, NonLinkedTableName)                                     ' delete temp table
                If NlErr < 0 Then                                                                               ' if error deleting table
                    Return NlErr                                                                                ' return the error
                End If
            End If

            ChildTableName = ChildTable.TableName

            ' CREATE TABLE NonLinked LIKE ChildTableName

            Dim createSql As String = String.Format("CREATE TABLE {0} LIKE {1}", NonLinkedTableName, ChildTableName)
            Dim createErr As Integer = MySqlTools.ExecuteNonQuery(MySqlConn, createSql)                         ' create non linked table like child
            If createErr < 0 Then                                                                               ' if had an error
                Return createErr                                                                                ' return error
            End If

            Dim ParentTableName As String = ChildTable.ParentRelations(0).ParentTable.TableName                 ' get parent table name

            ' INSERT INTO NonLinked (ColumnName1, ColumnName2, ...)
            ' SELECT ChildTableName.ColumnName1, ChildTableName.ColumnName2, ...
            ' FROM ChildTableName LEFT JOIN ParentTableName 
            '       ON ChildTableName.ChildLinkColumnName = ParentTableName.ParentLinkColumnName
            ' WHERE ((ParentTableName.ParentLinkColumnName) Is Null)            

            Dim ColumnNamesStr As String = MySqlTools.GetColumnNames(MySqlConn, ChildTable, ChildTableName)     ' get child columns as one string
            If ColumnNamesStr = String.Empty Then                                                               ' if no child columns
                Return teOtherErr                                                                               ' exit now
            End If
            Dim justColNamesStr As String = ColumnNamesStr.Replace(ChildTableName & ".", "")                    ' get child cols w/o table name

            Dim InsertSql As String = String.Format("INSERT INTO {0} ({1})", NonLinkedTableName, justColNamesStr)
            Dim SelectSQL As String = String.Format(" SELECT {0}", ColumnNamesStr)

            Dim ParentLinkColumnName As String =
                        ChildTable.ParentRelations(0).ChildKeyConstraint.RelatedColumns(0).ColumnName
            Dim ChildLinkColumnName As String =
                        ChildTable.ParentRelations(0).ChildKeyConstraint.Columns(0).ColumnName
            Dim FromSql As String = String.Format(" FROM {0} LEFT JOIN {1} ON {0}.{2} = {1}.{3}", ChildTableName, ParentTableName, ChildLinkColumnName, ParentLinkColumnName)

            Dim WhereSql As String = String.Format(" WHERE (({0}.{1}) Is Null)", ParentTableName, ParentLinkColumnName)

            ' returns # of rows inserted or error value
            Return MySqlTools.ExecuteNonQuery(MySqlConn, InsertSql & SelectSQL & FromSql & WhereSql)
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return teOtherErr
        End Try
    End Function

    Private Shared Function DeleteNonLinkedRows(MySqlConn As MySqlConnection,
                                                ChildTable As DataTable) As Integer

        ' Deletes non linked rows from a table.  If no temp save table exists, then one is created.  
        ' The data saved will be the non linked rows in ChildTable.  The temp table will be named 
        ' "ChildTableName" & "Nl".   If temp table does exist, then the non linked rows will be inserted
        ' into the temp table
        '
        ' note: this function assumes:
        '   1) only 1 parent relation, so can use zero index in aTable.ParentRelations
        '   2) only one column from the parent table is linked to one column in the child table
        '
        ' vars passed:
        '   MySqlConn - MySql connection
        '   ChildTable - table to delete non linked rows from
        '
        ' returns: 
        '   NoErrors - no errors
        '   teNoTableErr - no table
        '   nlNoParentRels - no parent relations
        '   nlMultiParentRels - multiple parent relations
        '   nlNoLinkedColumnsErr - no linked column in parent or child table
        '   nlMultiLinkedColumnsErr - multiple linked columns in parent ot child table
        '   teParametersErr - no value for parameter
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlErr- other error in sql 
        '   sqlNoTableName - from or to table name not set
        '   sqlErr- other error in sql 
        '   sqlMakeTableErr - error in make table sql 
        '   sqlDropErr - other error in dropping table
        '   sqlRenameTableErr - other error in rename
        '   teOtherErr - other error 

        Const DeleteSQL As String = "DELETE "

        Try
            Dim ChildTableName As String = String.Empty
            Dim NlErr As Integer = CheckNonLinkedTable(ChildTable)                                  ' make sure table is ok to remove non linked rows
            If NlErr <> NoErrors Then                                                               ' if not OK
                Return NlErr                                                                        ' return error 
            End If

            ChildTableName = ChildTable.TableName
            Dim ChildNlTableName As String = ChildTableName & NonLinkedAdded

            ' create the non linked rows table
            Dim NlRowCount As Integer = CreateNonLinked(MySqlConn, ChildTable, False)               ' create temp non linked table
            If NlRowCount < 0 Then                                                                  ' if got an error
                Return NlRowCount                                                                   ' return the error
            End If
            If NlRowCount = 0 Then                                                                  ' if no non linked rows
                _errorMessage = String.Format("No ""Non Linked"" rows found in table ""{0}"".", ChildTableName)
                _errorCode = NoErrors
                Return NoErrors                                                                     ' return no errors
            End If

            If TableExists(MySqlConn, NonLinkedTableName) <> ExistsTypes.Yes Then                   ' if no NonLinked table, then exit now
                _errorMessage = String.Format("Table ""{0}"". not found", NonLinkedTableName)
                _errorCode = teNoTableErr
                Return teNoTableErr                                                                 ' return error
            End If

            ' DELETE 
            ' FROM ChildTableName
            ' WHERE ChildKeyColumnName IN (SELECT ChildKeyColumnName FROM NonLinked)

            Dim FromSql As String = " FROM " & ChildTableName
            Dim ChildKeyColumnName As String = ChildTable.PrimaryKey(0).ColumnName
            Dim WhereSql As String = String.Format(" WHERE {0} IN (SELECT {0} FROM {1})", ChildKeyColumnName, NonLinkedTableName)

            NlRowCount = MySqlTools.ExecuteNonQuery(MySqlConn, DeleteSQL & FromSql & WhereSql)
            If NlRowCount < 0 Then                                                                  ' if error deleting non linked rows
                Return NlRowCount                                                                   ' return error
            End If

            ' save the non linked rows
            Dim GotTempTable As ExistsTypes = TableExists(MySqlConn, ChildNlTableName)              ' see if got temp table
            If GotTempTable = ExistsTypes.No Then                                                   ' if no temp table yet
                NlErr = RenameTable(MySqlConn, NonLinkedTableName, ChildNlTableName)                ' rename NonLinked to ChildNL
            ElseIf GotTempTable = ExistsTypes.Yes Then                                              ' else got child nl table
                NlErr = InsertNonLinkedRows(MySqlConn, ChildTable, ChildNlTableName)                ' save nl rows to child nl
            Else
                NlErr = teOtherErr
            End If
            If NlErr < 0 Then                                                                       ' if got an error in rename or Insert
                Return NlErr                                                                        ' return the error
            End If

            If TableExists(MySqlConn, NonLinkedTableName) = ExistsTypes.Yes Then                    ' if got temp NonLinked table, need to delete it
                NlErr = DropTable(MySqlConn, NonLinkedTableName)                                    ' delete temp table
                If NlErr < 0 Then                                                                   ' if error deleting table
                    Return NlErr                                                                    ' return the error
                End If
            End If

            Return NoErrors                     ' if got here, then deleted non linked rows   
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return teOtherErr
        End Try
    End Function

    Public Shared Function FixConstraintException(MySqlConn As MySqlConnection,
                                                  Table As DataTable) As Integer

        ' fixes problem in for non linked rows in table when filling table
        ' non linked rows are moved from table and saved in non linked rows table
        '
        ' vars passed:
        '   MySqlConn - MySql connection
        '   Table - table to fix
        '
        ' returns:
        '   NoErrors - table had non linked rows moved to separate table
        '   teConcurrencyErr -
        '     - table is a temp table 
        '     - currently fixing non linked rows for another table
        '     - could not move non linked rows
        '   teNoParentRows - no parent table
        '   teOtherErr - something went wrong

        Try
            _fixedMessage = String.Empty
            If MySqlTools.TableExists(MySqlConn, Table.TableName) <> MySqlTools.ExistsTypes.Yes Then
                _errorMessage = String.Format("Data table ""{0}"" has unlinked rows or duplicate key values.  It is a temporary table and cannot be fixed.  Table was not loaded.", Table.TableName)
                _errorCode = teConcurrencyErr
                Return teConcurrencyErr
            End If
            If FixingNonLinkedRows Then                                             ' if current fixing
                _errorMessage = "Already fixing Non Linked rows, cannot fix other table at this time."
                _errorCode = teConcurrencyErr
                Return teConcurrencyErr
            End If
            Dim ParentTableName As String = String.Empty
            If ParentRowCount(Table, ParentTableName) = 0 Then                      ' if no parent table rows
                _errorMessage = String.Format("Data table ""{0}"" is a child table of ""{1}"".  Parent table ""{1}"" has no rows.", Table.TableName, ParentTableName)
                _errorCode = teNoParentRows
                Return teNoParentRows
            End If
            FixingNonLinkedRows = True                                              ' now fixing table
            If DeleteNonLinkedRows(MySqlConn, Table) <> NoErrors Then                          ' if did not delete non linked rows
                Return teConstraintErr
            End If

            _fixedMessage = String.Format("Table ""{0}"" fixed.  Non linked rows have been moved to table ""{0}{1}"".", Table.TableName, NonLinkedAdded)
            Return NoErrors
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = teOtherErr
            Return teOtherErr
        End Try
    End Function

    Private Shared Function InsertNonLinkedRows(MySqlConn As MySqlConnection,
                                                ChildTable As DataTable,
                                                ChildNlTableName As String) As Integer

        ' inserts the rows from NonLinked table in to temp child table
        '
        ' vars passed: 
        '   ChildTable - table to delete non linked rows from
        '   ChildNlTableName - name of child temp table to save non linked rows 
        '
        ' returns: 
        '   NoErrors - no errors         
        '   teNoTableErr - no table
        '   teActualColumnsErr - could not get actual columns for child table
        '   <0 - SqlCommand.ExecuteNonQuery error value
        '   sqlErr- other error in sql 
        '   teOtherErr - other error 

        Try
            If ChildTable Is Nothing Then               ' if no value in parameter
                Return teNoTableErr                     ' return no table error
            End If
            If ChildNlTableName = String.Empty Then     ' if no value in parameter
                Return teNoTableErr                     ' return no table error
            End If

            ' INSERT INTO ChildNlTableName ( ColumnName1, ColumnName2, ... )
            ' SELECT NonLinked.ColumnName1, NonLinked.ColumnName2, ...
            ' FROM NonLinked

            Dim ColumnNamesStr As String = MySqlTools.GetColumnNames(MySqlConn, ChildTable, NonLinkedTableName) ' get child columns as one string
            If ColumnNamesStr = String.Empty Then                                                               ' if no child columns
                Return teActualColumnsErr                                                                       ' exit now
            End If
            Dim justColNamesStr As String = ColumnNamesStr.Replace(NonLinkedTableName & ".", "")                ' get child cols w/o table name

            Dim InsertSql As String = String.Format("INSERT INTO {0} ({1}) ", ChildNlTableName, justColNamesStr)
            Dim SelectSql As String = String.Format("SELECT {0} ", ColumnNamesStr)
            Dim FromSql As String = "FROM " & NonLinkedTableName

            Return MySqlTools.ExecuteNonQuery(MySqlConn, InsertSql & SelectSql & FromSql)
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = teOtherErr
            Return teOtherErr
        End Try
    End Function

    Private Shared Function ParentRowCount(ChildTable As DataTable,
                                           ByRef ParentTableName As String) As Integer

        ' gets the # of rows in the parent table in the data set (filled rows in the parent table)
        '
        ' vars passed:
        '   ChildTable - child data table with check parent table to check
        '   ParentTableName - child data table's parent table name (pass String.Empty, will be set to child data table's parent table name)
        '
        ' returns:
        '   >=0 - # of rows for parent table
        '   teNoTableErr - no table
        '   nlNoParentRels - no parent relations
        '   nlMultiParentRels - multiple parent relations
        '   teOtherErr - something went wrong

        Try
            Dim ChildTableName As String = String.Empty
            If ChildTable Is Nothing Then
                Return teNoTableErr
            End If
            ChildTableName = ChildTable.TableName

            ' make sure got only 1 parent relation
            If ChildTable.ParentRelations.Count = 0 Then
                EaTools.Tools.ShowErrorMessage(String.Format("No Parent Relation for table ""{0}"".", ChildTableName), etFixingConstraintErr)
                Return nlNoParentRelsErr
            End If
            If ChildTable.ParentRelations.Count > 1 Then
                EaTools.Tools.ShowErrorMessage(String.Format("Multiple Parent Relation for table ""{0}"".", ChildTableName), etFixingConstraintErr)
                Return nlMultiParentRelsErr
            End If
            ParentTableName = ChildTable.ParentRelations(0).ParentTable.TableName                   ' set parent table name
            Return ChildTable.ParentRelations(0).ParentTable.Rows.Count                             ' return # rows in the parent datatable
        Catch ex As Exception
            _errorMessage = String.Format(efOther, ex.Message)
            _errorCode = ex.HResult
            Return teOtherErr
        End Try
    End Function

#End Region

End Class
