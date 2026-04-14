# Real Attendance Web System

This is a local web app for attendance management.

## Features

- Employee management
- Daily shift planning
- Daily attendance check-in/check-out
- Automatic late/on-time status
- Worked-hours calculation (including overnight check-out)
- Excel export report (`Detailed` + `Summary`)

## Run

```powershell
cd C:\Users\B0008\Desktop\attendance_system
python app.py
```

Then open:

- http://127.0.0.1:5000

## Pages

- `/employees` add employees
- `/shifts` add shift by date
- `/attendance` add attendance by date
- `/reports` view full report and export Excel