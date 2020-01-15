Public Class MySqlParam

    Public Const mode_IN As String = "IN"
    Public Const mode_INOUT As String = "INOUT"
    Public Const mode_OUT As String = "OUT"

    Private _mode As String = mode_IN

    Public Sub New(name As String)
        Me.Name = name
    End Sub

    Public Sub New(name As String, column As MySqlColumn)

        ' Name - parameter/variable name
        ' Column - data column for parameter/variable

        Me.Name = name
        Me.Column = column
    End Sub

    Public Sub New(name As String, value As Object)

        ' Name - parameter/variable name        
        ' value - value for parameter/variable

        Me.Name = name
        Me.Value = value
    End Sub

    Public Sub New(name As String, column As MySqlColumn, value As Object)

        ' Name - parameter/variable name
        ' Column - data column for parameter/variable
        ' value - value for parameter/variable

        Me.New(name, column)
        Me.Value = value
    End Sub

    Public Sub New(name As String, column As MySqlColumn, value As Object, mode As String)
        Me.New(name, column, value)
        Me.Mode = mode
    End Sub

    Public Sub New(Param As MySqlParam)
        Me.New(Param.Name, Param.Column, Param.Value, Param.Mode)
    End Sub

    Public Property Column As MySqlColumn = Nothing
    Public ReadOnly Property DataType As MySqlDataType
        Get
            Return Column.DataType
        End Get
    End Property
    Public Property Mode As String
        Get
            Return _mode
        End Get
        Set(value As String)
            value = value.ToUpper
            If value = mode_INOUT OrElse value = mode_OUT Then
                _mode = value
            Else
                _mode = "IN"
            End If
        End Set
    End Property
    Public Property Name As String
    Public Property Value As Object = Nothing
End Class
