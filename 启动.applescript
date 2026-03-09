on run
    try
        set appPath to POSIX path of (path to me)
        set projectDir to do shell script "dirname " & quoted form of appPath
        set runner to projectDir & "/launch_no_terminal.sh"
        do shell script quoted form of runner
    on error errMsg
        display alert "启动失败" message errMsg as critical
    end try
end run
