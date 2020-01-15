Public Class IndexColumnInfo

    Public Sub New(mySqlCol As MySqlColumn, sortASC As Boolean)
        Me.MySqlCol = mySqlCol
        Me.SortASC = sortASC
    End Sub

    ReadOnly Property MySqlCol As MySqlColumn
    ReadOnly Property SortASC As Boolean
End Class
