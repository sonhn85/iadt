Set oShell = CreateObject("Wscript.Shell")
Dim strArgs
strArgs = "cmd /c ..\pyenv\python iadt\iadt.py"
oShell.Run strArgs, 0, false