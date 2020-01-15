Public NotInheritable Class MySqlDataType

    ' TypeName tn....

    Public Const tnBigInt As String = "BigInt"
    Public Const tnBoolean As String = "Boolean"
    Public Const tnDate As String = "Date"
    Public Const tnDateTime As String = "DateTime"
    Public Const tnDecimal As String = "Decimal"
    Public Const tnDouble As String = "Double"
    Public Const tnFloat As String = "Float"
    Public Const tnInteger As String = "Int"
    Public Const tnNone As String = "None"
    Public Const tnSmallInt As String = "SmallInt"
    Public Const tnText As String = "Text"
    Public Const tnTimeStamp As String = "TimeStamp"
    Public Const tnTinyInt As String = "TinyInt"
    Public Const tnVarChar As String = "VarChar"

    Public Const trillionWidth As Integer = 14  ' enough for 999,999,999,999.99 or a trillion $ - 0.01
    Public Const centsWidth As Integer = 2

    Private ReadOnly _name As String
    Private ReadOnly value As Integer

    Public Shared ReadOnly dtNone As New MySqlDataType(0, tnNone)
    Public Shared ReadOnly dtBigInt As New MySqlDataType(1, tnBigInt)
    Public Shared ReadOnly dtBoolean As New MySqlDataType(2, tnBoolean)
    Public Shared ReadOnly dtDate As New MySqlDataType(3, tnDate)
    Public Shared ReadOnly dtDateTime As New MySqlDataType(4, tnDateTime)
    Public Shared ReadOnly dtDecimal As New MySqlDataType(5, tnDecimal)
    Public Shared ReadOnly dtDouble As New MySqlDataType(6, tnDouble)
    Public Shared ReadOnly dtFloat As New MySqlDataType(7, tnFloat)
    Public Shared ReadOnly dtInteger As New MySqlDataType(8, tnInteger)
    Public Shared ReadOnly dtSmallInt As New MySqlDataType(9, tnSmallInt)
    Public Shared ReadOnly dtText As New MySqlDataType(10, tnText)
    Public Shared ReadOnly dtTimeStamp As New MySqlDataType(11, tnTimeStamp)
    Public Shared ReadOnly dtTinyInt As New MySqlDataType(1, tnTinyInt)
    Public Shared ReadOnly dtVarChar As New MySqlDataType(1, tnVarChar)

    Private Sub New(value As Integer, name As String)
        _name = name
        Me.value = value
    End Sub

    Public Overrides Function ToString() As String
        Return _name
    End Function
End Class
