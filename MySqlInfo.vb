Public Class MySqlInfo

#Region " Notes "

    ' this calls is used for passing data to DoesItExists

#End Region

#Region " Constants "

    Private Const efOther As String = "Other error: {0}"

#End Region

#Region " ENums "
    Enum MySqlTypesToFindTypes
        NotSet = -1
        Database = 0
        User = 1
        Table = 2
        TableInfo = 3
        Column = 4
        PrimaryKey = 5
        Index = 6
        Indexes = 7
        Procedure = 9
        ProcedureSqlText = 10
        ProcedureParams = 11
        View = 12
        ViewSqlText = 13
        Version = 14
    End Enum

#End Region

#Region " Private Vars "

    Private _text As String
    Private _names As List(Of String)
    Private _nameToFind As String
    Private _params As List(Of MySqlParam)
    Private _tableInfo As DataTable

#End Region

#Region " New "

    Public Sub New(mySqlTypeToFind As MySqlTypesToFindTypes,
                   nameToFind As String)

        ' vars passed:
        '   mySqlTypeToFind - MySql item type to find
        '   whatToFind - data to find (tableName, procedure name, view name...)

        Me.MySqlTypeToFind = mySqlTypeToFind
        _nameToFind = nameToFind
        If mySqlTypeToFind = MySqlTypesToFindTypes.Indexes Then
            _names = New List(Of String)
            Me.TableName = nameToFind
        ElseIf mySqlTypeToFind = MySqlTypesToFindTypes.TableInfo OrElse mySqlTypeToFind = MySqlTypesToFindTypes.PrimaryKey Then
            Me.TableName = nameToFind
        End If
    End Sub

    Public Sub New(mySqlTypeToFind As MySqlTypesToFindTypes,
                   nameToFind As String,
                   tableName As String)

        ' vars passed:
        '   mySqlTypeToFind - MySql item type to find
        '   nameToFind - name of item to find 
        '   tableName - name of table containing item to find

        Me.New(mySqlTypeToFind, nameToFind)
        Me.TableName = tableName
    End Sub

    Public Sub New(mySqlTypeToFind As MySqlTypesToFindTypes,
                   nameToFind As String,
                   tableName As String,
                   database As String)

        ' vars passed:
        '   mySqlTypeToFind - MySql item type to find
        '   nameToFind - name of item to find 
        '   tableName - name of table containing item to find
        '   database - name of database with tables

        Me.New(mySqlTypeToFind, nameToFind)
        Me.Database = database
        If mySqlTypeToFind = MySqlTypesToFindTypes.Table Then
            Me.TableName = tableName
            _nameToFind = String.Empty
        End If
    End Sub

#End Region

#Region " Properties "

    ReadOnly Property Database As String = String.Empty
    ReadOnly Property MySqlTypeToFind As MySqlTypesToFindTypes
    ReadOnly Property ReaderData As Object
        Get
            Select Case MySqlTypeToFind
                Case MySqlTypesToFindTypes.TableInfo
                    Return _tableInfo
                Case MySqlTypesToFindTypes.Indexes
                    Return _names
                Case MySqlTypesToFindTypes.ProcedureSqlText,
                     MySqlTypesToFindTypes.ViewSqlText,
                     MySqlTypesToFindTypes.Version
                    Return _text
                Case MySqlTypesToFindTypes.ProcedureParams
                    Return _params
                Case Else
                    Return Nothing
            End Select
        End Get
    End Property
    ReadOnly Property NameToFind As String
        Get
            Return _nameToFind
        End Get
    End Property
    ReadOnly Property SqlText As String
        Get
            Select Case MySqlTypeToFind

                Case MySqlTypesToFindTypes.Database

                    ' SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '<database>'
                    Const DatabaseExistsFormat As String = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{0}'"
                    Return String.Format(DatabaseExistsFormat, NameToFind.ToLower)

                Case MySqlTypesToFindTypes.User

                    ' SELECT COUNT(*) FROM mysql.user WHERE user='<username>'
                    Const UserExistsFormat As String = "SELECT COUNT(*) FROM mysql.user WHERE user='{0}'"
                    Return String.Format(UserExistsFormat, NameToFind.ToLower)

                Case MySqlTypesToFindTypes.Table

                    ' SHOW TABLES IN `<database>` LIKE '<tablename>'
                    Const TableExistsFormat As String = "SHOW TABLES IN `{0}` LIKE '{1}'"
                    Return String.Format(TableExistsFormat, Database.ToLower, TableName.ToLower)

                Case MySqlTypesToFindTypes.TableInfo

                    ' SELECT * FROM `<tablename>` LIMIT 1
                    Const SelectOneRowFormat As String = "SELECT * FROM `{0}` LIMIT 1"
                    Return String.Format(SelectOneRowFormat, TableName.ToLower)

                Case MySqlTypesToFindTypes.Column

                    ' SHOW COLUMNS FROM `<tablename>` LIKE '<ColumnName>'
                    Const ColumnExistsFormat As String = "SHOW COLUMNS FROM `{0}` LIKE '{1}'"
                    Return String.Format(ColumnExistsFormat, TableName.ToLower, NameToFind)

                Case MySqlTypesToFindTypes.PrimaryKey

                    ' SHOW KEYS FROM `<tablename>` WHERE Key_name = 'PRIMARY'
                    Const PrimaryKeySqlText As String = "SHOW KEYS FROM `{0}` WHERE Key_name = 'PRIMARY'"
                    Return String.Format(PrimaryKeySqlText, TableName.ToLower)

                Case MySqlTypesToFindTypes.Index

                    ' SHOW INDEX FROM `<tablename>` WHERE Key_name = '<indexname>'
                    Const IndexExistsFormat As String = "SHOW INDEX FROM `{0}` WHERE Key_name = '{1}'"

                    Return String.Format(IndexExistsFormat, TableName.ToLower, NameToFind)

                Case MySqlTypesToFindTypes.Indexes
                    ' SHOW INDEX FROM `<tablename>` FROM `<database>`
                    ' SHOW INDEX FROM `<tablename>`
                    Const IndexesExistsFormat As String = "SHOW INDEX FROM `{0}` "
                    Return String.Format(IndexesExistsFormat, TableName.ToLower, NameToFind)

                Case MySqlTypesToFindTypes.Procedure

                    ' SHOW PROCEDURE STATUS WHERE `Name` = "<ProcName>"
                    Const ProcExistsFormat As String = "SHOW PROCEDURE STATUS WHERE `Name` = ""{0}"""
                    Return String.Format(ProcExistsFormat, NameToFind)

                Case MySqlTypesToFindTypes.ProcedureSqlText

                    ' SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '<database>' AND ROUTINE_NAME = '<ProcName>'
                    Const ProcTextFormat As String = "SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '{0}' AND ROUTINE_NAME = '{1}'"
                    Return String.Format(ProcTextFormat, Database.ToLower, _nameToFind.ToLower)

                Case MySqlTypesToFindTypes.ProcedureParams

                    ' SELECT * FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_SCHEMA = '<database>' AND SPECIFIC_NAME = '<ProcName>'
                    Const ProcVarsFormat As String = "SELECT * FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_SCHEMA = '{0}' AND SPECIFIC_NAME = '{1}'"
                    Return String.Format(ProcVarsFormat, Database.ToLower, _nameToFind.ToLower)

                Case MySqlTypesToFindTypes.View

                    ' use ` to delimitate database name, and use ' to delimitate VIEW
                    ' SHOW FULL TABLES IN `<database>` WHERE TABLE_TYPE LIKE 'VIEW'
                    Const ViewExistsFormat As String = "SHOW FULL TABLES IN `{0}` WHERE TABLE_TYPE LIKE 'VIEW'"
                    Return String.Format(ViewExistsFormat, Database.ToLower)

                Case MySqlTypesToFindTypes.ViewSqlText

                    ' SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '<database>' AND TABLE_NAME = '<viewname>'
                    Const ViewTextFormat As String = "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'"
                    Return String.Format(ViewTextFormat, Database.ToLower, _nameToFind.ToLower)

                Case MySqlTypesToFindTypes.Version

                    ' SELECT VERSION()
                    ' SHOW VARIABLES LIKE "%version%"
                    Const VersionStr As String = "SELECT VERSION()"
                    Return VersionStr

                Case Else
                    Return String.Empty
            End Select
        End Get
    End Property
    ReadOnly Property TableName As String = String.Empty
#End Region

#Region " Methods "

    Public Function DoesItExits(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if items exists 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   ErrorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - it was found
        '   ExistsTypes.No - it was not found        
        '   ExistsTypes.GotError - something went wrong

        Try
            Select Case MySqlTypeToFind
                Case MySqlTypesToFindTypes.Column,
                     MySqlTypesToFindTypes.Database,
                     MySqlTypesToFindTypes.Index,
                     MySqlTypesToFindTypes.Table,
                     MySqlTypesToFindTypes.Procedure
                    Return ExistsIfGotReader(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.TableInfo
                    Return TableInfoExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.Indexes
                    Return CountIndexes(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.PrimaryKey
                    Return PrimaryKeyExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.User
                    Return UserExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.ProcedureSqlText,
                     MySqlTypesToFindTypes.ViewSqlText
                    Return ProcViewTextExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.ProcedureParams
                    Return ProcParamsExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.View
                    Return ViewExists(ReaderObj, errorMessage)
                Case MySqlTypesToFindTypes.Version
                    Return VersionExists(ReaderObj, errorMessage)
                Case Else
                    Return MySqlTools.ExistsTypes.GotError
            End Select
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

#End Region

#Region " Private Functions "

    Private Function ExistsIfGotReader(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                       ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if item exists 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - it was found
        '   ExistsTypes.No - it was not found        
        '   ExistsTypes.GotError - something went wrong

        Try
            If ReaderObj Is Nothing Then                                    ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                         ' cannot confirm database exists
            Else                                                            ' else got a return value             
                If ReaderObj.HasRows Then                                   ' if got rows in return object
                    Return MySqlTools.ExistsTypes.Yes                          ' database exists
                Else                                                        ' else no rows
                    Return MySqlTools.ExistsTypes.No                           ' database does not exist
                End If
            End If
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function CountIndexes(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                  ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if indexes for table exist, and populates _names
        '
        ' note: populates _names 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - indexes were found 
        '   ExistsTypes.No - indexes were not found        
        '   ExistsTypes.GotError - something went wrong

        Const IndexNameIndex As Integer = 2

        Try
            Dim curName As String                                           ' current found index name
            If ReaderObj Is Nothing Then                                    ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                         ' cannot confirm user exists
            Else                                                            ' else got a return value  
                If ReaderObj.HasRows Then                                   ' if got rows in return object
                    While ReaderObj.Read()                                  ' get the next piece of data
                        curName = ReaderObj.GetString(IndexNameIndex)       ' get the currently found index name
                        If _names.IndexOf(curName) < 0 Then                 ' if current index name not yet found
                            _names.Add(curName)                             ' add name to list of found indexes
                        End If
                    End While
                End If
                If _names.Count > 0 Then                                    ' if got indexes
                    Return MySqlTools.ExistsTypes.Yes                          ' indexes do exist
                Else                                                        ' else no rows
                    Return MySqlTools.ExistsTypes.No                           ' indexes do not exist
                End If
            End If
        Catch ex As Exception
            _names.Clear()
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function GetDataType(DataTypeStr As String) As MySqlDataType

        Try
            Select Case DataTypeStr.ToUpper
                Case MySqlDataType.tnBigInt.ToUpper
                    Return MySqlDataType.dtBigInt
                Case MySqlDataType.tnBoolean.ToUpper
                    Return MySqlDataType.dtBoolean
                Case MySqlDataType.tnDate.ToUpper
                    Return MySqlDataType.dtDate
                Case MySqlDataType.tnDateTime.ToUpper
                    Return MySqlDataType.dtDateTime
                Case MySqlDataType.tnDecimal.ToUpper
                    Return MySqlDataType.dtDecimal
                Case MySqlDataType.tnDouble.ToUpper
                    Return MySqlDataType.dtDouble
                Case MySqlDataType.tnFloat.ToUpper
                    Return MySqlDataType.dtFloat
                Case MySqlDataType.tnInteger.ToUpper
                    Return MySqlDataType.dtInteger
                Case MySqlDataType.tnNone.ToUpper
                    Return MySqlDataType.dtNone
                Case MySqlDataType.tnSmallInt.ToUpper
                    Return MySqlDataType.dtSmallInt
                Case MySqlDataType.tnText.ToUpper
                    Return MySqlDataType.dtText
                Case MySqlDataType.tnTimeStamp.ToUpper
                    Return MySqlDataType.dtTimeStamp
                Case MySqlDataType.tnTinyInt.ToUpper
                    Return MySqlDataType.dtTinyInt
                Case MySqlDataType.tnVarChar.ToUpper
                    Return MySqlDataType.dtVarChar
                Case Else
                    Return MySqlDataType.dtNone
            End Select
        Catch ex As Exception
            Return MySqlDataType.dtNone
        End Try
    End Function

    Private Function PrimaryKeyExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                      ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if Primary key exists, and sets PrimaryKey Name in NameToFind property
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - Primary key was found 
        '   ExistsTypes.No - Primary key was not found        
        '   ExistsTypes.GotError - something went wrong

        Const pkNameColIndex As Integer = 2

        Try
            If ReaderObj Is Nothing Then                                    ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                         ' cannot confirm user exists
            Else                                                            ' else got a return value                             
                If ReaderObj.HasRows Then                                   ' if got rows in return object
                    ReaderObj.Read()                                        ' only need to read one row
                    _nameToFind = CType(ReaderObj(pkNameColIndex), String)  ' get primary key name
                    Return MySqlTools.ExistsTypes.Yes                          ' primary key exists
                Else                                                        ' else count will be 0
                    Return MySqlTools.ExistsTypes.No                           ' primary key does not exist
                End If
            End If
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function ProcParamsExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                      ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if proc parameters exist, and sets _params
        '
        ' note: sets _params
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - view was found 
        '   ExistsTypes.No - view was not found        
        '   ExistsTypes.GotError - something went wrong

        Try
            If ReaderObj Is Nothing Then                                                ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                                     ' cannot confirm view exists
            Else                                                                        ' else got a reader object
                If ReaderObj.HasRows Then                                               ' if got rows in return object
                    _params = New List(Of MySqlParam)
                    Dim dataType As MySqlDataType
                    Dim dataTypeStr As String
                    Dim maxLen As Integer
                    Dim mode As String
                    Dim mySqlCol As MySqlColumn
                    Dim name As String
                    Dim numPrecision As Integer
                    Dim numScale As Integer
                    Dim ordPos As Integer
                    While ReaderObj.Read()                                              ' get the next piece of data
                        ordPos = ReaderObj.GetInt32(MySqlTools.cnOrdinalPos)
                        mode = ReaderObj.GetString(MySqlTools.cnParamMode)
                        name = ReaderObj.GetString(MySqlTools.cnParamName)
                        dataTypeStr = ReaderObj.GetString(MySqlTools.cnDataType)
                        dataType = GetDataType(dataTypeStr)
                        If dataType Is MySqlDataType.dtDecimal Then                     ' if a decimal value
                            numPrecision = ReaderObj.GetInt32(MySqlTools.cnNumPrecision)   ' get precision
                            numScale = ReaderObj.GetInt32(MySqlTools.cnNumScale)           ' get scale
                            mySqlCol = New MySqlColumn(name, numPrecision, numScale)    ' create decimal column
                        ElseIf dataType Is MySqlDataType.dtVarChar Then                 ' else if a var char value
                            maxLen = ReaderObj.GetInt32(MySqlTools.cnCharMaxLen)           ' get max length
                            mySqlCol = New MySqlColumn(name, dataType, maxLen)          ' create var char column
                        Else                                                            ' else other type of value
                            mySqlCol = New MySqlColumn(name, dataType)                  ' create column no type needed
                        End If
                        _params.Add(New MySqlParam(name, mySqlCol, Nothing, mode))
                    End While
                    If _params.Count > 0 Then
                        Return MySqlTools.ExistsTypes.Yes                                  ' params exits
                    Else
                        Return MySqlTools.ExistsTypes.No                                   ' did not find match, params do not exits
                    End If
                Else                                                                    ' else no rows
                    Return MySqlTools.ExistsTypes.No                                       ' view does not exist
                End If
            End If

        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function ProcViewTextExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                        ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if proc/view exist, and sets _text
        '
        ' note: sets _text
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - view was found 
        '   ExistsTypes.No - view was not found        
        '   ExistsTypes.GotError - something went wrong

        Const DefIndex As Integer = 0

        Try
            If ReaderObj Is Nothing Then                                                ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                                     ' cannot confirm view exists
            Else                                                                        ' else got a reader object
                If ReaderObj.HasRows Then                                               ' if got rows in return object
                    While ReaderObj.Read()                                              ' get the next piece of data
                        _text = ReaderObj.GetString(DefIndex)                           ' return string in create view/proc column
                        Return MySqlTools.ExistsTypes.Yes                                  ' view does exits
                    End While
                    Return MySqlTools.ExistsTypes.No                                       ' did not find match, view does not exits
                Else                                                                    ' else no rows
                    Return MySqlTools.ExistsTypes.No                                       ' view does not exist
                End If
            End If
        Catch ex As Exception
            _text = String.Empty
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function TableInfoExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                     ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if table info exist, and sets _tableInfo
        '
        ' note: sets _tableInfo
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - table info was found 
        '   ExistsTypes.No - table info was not found        
        '   ExistsTypes.GotError - something went wrong

        Try
            If ReaderObj Is Nothing Then                                    ' if something went wrong
                errorMessage = "No Reader Object"
                _tableInfo = Nothing                                        ' no table info
                Return MySqlTools.ExistsTypes.GotError                         ' cannot confirm table info exists
            Else                                                            ' else got a reader object
                If ReaderObj.HasRows Then                                   ' if got rows in return object
                    _tableInfo = ReaderObj.GetSchemaTable                   ' return the schema table
                    Return MySqlTools.ExistsTypes.Yes
                Else                                                        ' else no rows
                    _tableInfo = Nothing                                    ' no table info
                    Return MySqlTools.ExistsTypes.No                           ' table info does not exist
                End If
            End If
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            _tableInfo = Nothing
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function UserExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if user exists 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - user was found
        '   ExistsTypes.No - user was not found        
        '   ExistsTypes.GotError - something went wrong

        Const UserIndex As Integer = 0

        Try
            If ReaderObj Is Nothing Then                                ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                     ' cannot confirm user exists
            Else                                                        ' else got a return value                             
                If ReaderObj.HasRows Then                               ' if got rows in return object
                    ReaderObj.Read()                                    ' only need to read one row
                    If ReaderObj.GetInt32(UserIndex) = 1 Then           ' if count is 1
                        Return MySqlTools.ExistsTypes.Yes                  ' user exists
                    Else                                                ' else count will be 0
                        Return MySqlTools.ExistsTypes.No                   ' user does not exist
                    End If
                Else                                                    ' else no rows
                    Return MySqlTools.ExistsTypes.No                       ' user does not exist
                End If
            End If
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function VersionExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                   ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if version info, and sets _nameToFind
        '
        ' note: sets _nameToFind 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - version info was found 
        '   ExistsTypes.No - version info was not found        
        '   ExistsTypes.GotError - something went wrong

        Const rawViewIndex As Integer = 0

        Try
            If ReaderObj Is Nothing Then                                ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                     ' cannot confirm view exists
            Else                                                        ' else got a return value                             
                If ReaderObj.HasRows Then                               ' if got rows in return object
                    ReaderObj.Read()                                    ' get the next piece of data
                    _text = ReaderObj.GetString(rawViewIndex)           ' get version string
                    Return MySqlTools.ExistsTypes.Yes                      ' view does exits
                Else                                                    ' else no rows
                    Return MySqlTools.ExistsTypes.No                       ' view does not exist
                End If
            End If
        Catch ex As Exception
            _nameToFind = String.Empty
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

    Private Function ViewExists(ReaderObj As MySql.Data.MySqlClient.MySqlDataReader,
                                ByRef errorMessage As String) As MySqlTools.ExistsTypes

        ' checks if view exists 
        ' 
        ' vars passed:
        '   ReaderObj - value returned from MySql.Data.MySqlClient.MySqlCommand.ExecuteReader
        '   errorMessage - PASSED ByRef - error message when error occurs
        '
        ' returns:
        '   ExistsTypes.Yes - view was found
        '   ExistsTypes.No - view was not found        
        '   ExistsTypes.GotError - something went wrong

        Const NameIndex As Integer = 0

        Try
            If ReaderObj Is Nothing Then                                                ' if something went wrong
                errorMessage = "No Reader Object"
                Return MySqlTools.ExistsTypes.GotError                                     ' cannot confirm view exists
            Else                                                                        ' else got a return value                             
                If ReaderObj.HasRows Then                                               ' if got rows in return object
                    While ReaderObj.Read()                                              ' get the next piece of data
                        If ReaderObj.GetString(NameIndex) = NameToFind.ToLower Then     ' if the view name matches 
                            Return MySqlTools.ExistsTypes.Yes                              ' view does exits
                        End If
                    End While
                    Return MySqlTools.ExistsTypes.No                                       ' did not find match, view does not exits
                Else                                                                    ' else no rows
                    Return MySqlTools.ExistsTypes.No                                       ' view does not exist
                End If
            End If
        Catch ex As Exception
            errorMessage = String.Format(efOther, ex.Message)
            Return MySqlTools.ExistsTypes.GotError
        End Try
    End Function

#End Region

End Class
