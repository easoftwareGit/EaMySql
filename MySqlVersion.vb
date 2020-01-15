Public Class MySqlVersion

    Private _build As Integer
    Private _major As Integer
    Private _minor As Integer

    Public ReadOnly Property Build As Integer
        Get
            Return _build
        End Get
    End Property
    Public ReadOnly Property Major As Integer
        Get
            Return _major
        End Get
    End Property
    Public ReadOnly Property Minor As Integer
        Get
            Return _minor
        End Get
    End Property

    Public Sub New()
        _build = 0
        _major = 0
        _minor = 0
    End Sub

    Public Sub New(versionStr As String)
        Me.New()
        Dim ValueStrs() As String = versionStr.Split("."c)
        If versionStr.Length > 0 Then
            For i As Integer = 0 To versionStr.Length - 1
                Select Case i
                    Case 0
                        _major = GetVerionNumber(ValueStrs(i))
                    Case 1
                        _minor = GetVerionNumber(ValueStrs(i))
                    Case 2
                        _build = GetVerionNumber(ValueStrs(i))
                    Case Else
                        Return
                End Select
            Next
        End If
    End Sub

    Private Function GetVerionNumber(verStr As String) As Integer
        Try
            Return CInt(verStr)
        Catch ex As Exception
            Return 0
        End Try
    End Function

End Class
