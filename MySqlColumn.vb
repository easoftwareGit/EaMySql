Public Class MySqlColumn

#Region " Class Constants and Variables "

#Region " Constants "

#End Region

#Region " Enums "

#End Region

#Region " Private Vars "

    Private _autoInc As Boolean = False
    Private _columnName As String
    Private _dataType As MySqlDataType
    Private _defaultValue As String
    Private _keyField As Boolean = False
    Private _isNullable As Boolean = True
    Private _length As Integer
    Private _onUpdate As String = String.Empty
    Private _precision As Integer

#End Region

#End Region

#Region " New "

    Public Sub New(columnName As String,
                   columnType As MySqlDataType,
                   Optional autoInc As Boolean = False,
                   Optional keyField As Boolean = False,
                   Optional defaultValue As String = "",
                   Optional isNullable As Boolean = True)

        ' vars passed:
        '   aColumnName - name for the column
        '   aColumnType - column type 
        '   aKeyField - is column a key field
        '   aDefaultValue - columns's default value as a string

        Me.ColumnName = columnName
        _autoInc = autoInc
        Me.DataType = columnType
        Me.KeyField = keyField
        Me.DefaultValue = defaultValue
        Me.IsNullable = isNullable
    End Sub

    Public Sub New(columnName As String,
                   columnType As MySqlDataType,
                   Length As Integer,
                   Optional keyField As Boolean = False,
                   Optional defaultValue As String = "",
                   Optional isNullable As Boolean = True)

        ' vars passed:
        '   ColumnName - name for the column
        '   ColumnType - column type 
        '   Length - length of text in a text column or total digits in decimal column
        '   aKeyField - is column a key field
        '   aDefaultValue - columns's default value as a string

        Me.New(columnName, columnType, False, keyField, defaultValue, isNullable)
        Me.Length = Length
    End Sub

    Public Sub New(columnName As String,
                   Length As Integer,
                   Precision As Integer,
                   Optional keyField As Boolean = False,
                   Optional defaultValue As String = "",
                   Optional isNullable As Boolean = True)

        ' vars passed:
        '   ColumnName - name for the column
        '   Length - total digits in decimal column
        '   Precision - number of digits right of decimal point
        '   TextLength - length of text in a text column
        '   KeyField - is column a key field
        '   DefaultValue - columns's default value as a string

        Me.New(columnName, MySqlDataType.dtDecimal, Length, keyField, defaultValue, isNullable)
        Me.Precision = Precision
    End Sub

#End Region

#Region " Properties "

    Property AutoInc As Boolean
        Get
            Return _autoInc
        End Get
        Set(value As Boolean)
            Try
                _autoInc = value
            Catch ex As Exception
                _autoInc = False
            End Try
        End Set
    End Property

    Property ColumnName() As String
        Get
            Return _columnName
        End Get
        Set(ByVal Value As String)
            Try
                _columnName = Value
            Catch ex As Exception
                _columnName = String.Empty
            End Try
        End Set
    End Property

    Property DataType() As MySqlDataType
        Get
            Return _dataType
        End Get
        Set(ByVal Value As MySqlDataType)
            Try
                If (Value Is MySqlDataType.dtBoolean) OrElse (Value Is MySqlDataType.dtDecimal) OrElse
                        (Value Is MySqlDataType.dtDate) OrElse (Value Is MySqlDataType.dtDateTime) OrElse
                        (Value Is MySqlDataType.dtTimeStamp) OrElse (Value Is MySqlDataType.dtFloat) OrElse
                        (Value Is MySqlDataType.dtDouble) OrElse (Value Is MySqlDataType.dtTinyInt) OrElse
                        (Value Is MySqlDataType.dtSmallInt) OrElse (Value Is MySqlDataType.dtInteger) OrElse
                        (Value Is MySqlDataType.dtBigInt) OrElse (Value Is MySqlDataType.dtText) OrElse
                        (Value Is MySqlDataType.dtVarChar) Then                         ' if a valid type
                    _dataType = Value                                                   ' then set column type
                Else                                                                    ' else type is none or not valid
                    _dataType = MySqlDataType.dtNone                                    ' return none
                End If
            Catch ex As Exception
                _dataType = MySqlDataType.dtNone
            End Try
        End Set
    End Property

    Property DefaultValue As String
        Get
            Return _defaultValue
        End Get
        Set(value As String)
            Try
                _defaultValue = value
            Catch ex As Exception
                _defaultValue = ""
            End Try
        End Set
    End Property

    Property KeyField() As Boolean
        Get
            Return _keyField
        End Get
        Set(ByVal Value As Boolean)
            Try
                If (_dataType Is MySqlDataType.dtNone) OrElse (_dataType Is MySqlDataType.dtText) Then      ' if col type is none or text
                    _keyField = False                                                                       ' cannot be key field
                Else                                                                                        ' else col can be key field
                    _keyField = Value                                                                       ' set key field property
                End If
            Catch ex As Exception
                _keyField = False
            End Try
        End Set
    End Property

    Property IsNullable As Boolean
        Get
            Return _isNullable
        End Get
        Set(value As Boolean)
            Try
                _isNullable = value
            Catch ex As Exception
                _isNullable = True
            End Try
        End Set
    End Property

    Property Length() As Integer
        Get
            Return _length
        End Get
        Set(ByVal Value As Integer)
            Try
                ' if col type is text or varChar or decimal
                If _dataType Is MySqlDataType.dtText OrElse _dataType Is MySqlDataType.dtVarChar OrElse _dataType Is MySqlDataType.dtDecimal Then
                    _length = Value                                         ' then set text length
                Else                                                        ' else not a a text or varChar col
                    _length = -1                                            ' set text length to -1
                End If
            Catch ex As Exception
                _length = -1
            End Try
        End Set
    End Property

    Property OnUpdate As String
        Get
            Return _onUpdate
        End Get
        Set(value As String)
            Try
                _onUpdate = value
            Catch ex As Exception
                _onUpdate = String.Empty
            End Try
        End Set
    End Property

    Property Precision As Integer
        Get
            Return _precision
        End Get
        Set(value As Integer)
            Try
                If _dataType Is MySqlDataType.dtDecimal Then
                    _precision = value
                Else
                    _precision = -1
                End If
            Catch ex As Exception
                _precision = -1
            End Try
        End Set
    End Property

    ReadOnly Property SqlDataType() As String
        Get
            Return _dataType.ToString
        End Get
    End Property

#End Region

#Region " Methods "

    Public Sub CopyTo(copyToCol As MySqlColumn)

        ' copies to another mySql column
        '
        ' vars passed:
        '   CoptToCol - column to copy to
        '

        Try
            copyToCol._autoInc = _autoInc
            copyToCol._columnName = _columnName
            copyToCol._dataType = _dataType
            copyToCol._defaultValue = _defaultValue
            copyToCol._isNullable = _isNullable
            copyToCol._keyField = _keyField
            copyToCol._length = _length
        Catch ex As Exception
            Return
        End Try
    End Sub

#End Region

End Class
