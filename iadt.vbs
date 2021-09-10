Set oShell = CreateObject("Wscript.Shell")
Dim strArgs
strArgs = "cmd /c pyenv\python.exe iadt\iadt.py"
oShell.Run strArgs, 0, false