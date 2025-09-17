# 🎵 MusicScope™ Professional Dashboard

## 🚀 Quick Start - PLAY BUTTON

**Want to create/update your dashboard? Follow these 2 steps:**

### Step 1: Check Dependencies
```bash
python 🔧_CHECK_DEPENDENCIES.py
```

### Step 2: Run the Dashboard
```bash
python 🚀_RUN_NOTEBOOK_CREATION.py
```

That's it! These are your "play buttons" for the entire system.

## 📁 What's in this directory

- **🚀_RUN_NOTEBOOK_CREATION.py** - Your PLAY BUTTON (run this!)
- **MusicScope™_Professional_Dashboard.ipynb** - Blueprint template
- **MusicScope™_Professional_Dashboard_YYYYMMDD_HHMMSS_executed.ipynb** - Current executed version
- **archive/** - Old executed versions (organized by date)

## 🔄 How it works

1. **📦 Archive**: Moves old executed notebooks to `archive/YYYYMMDD_HHMMSS/`
2. **🔄 Create**: Updates the blueprint with fresh data from your database
3. **🚀 Execute**: Runs the blueprint to generate 20 beautiful charts
4. **🔍 Validate**: Checks for errors and FAILS LOUDLY if issues found
5. **✅ Success**: Your dashboard is ready!

## 🎯 System maintains exactly 2 files

- **Blueprint** (clean template)
- **Executed** (current version with outputs)

All old versions are safely archived with timestamps.

## 🚨 If something goes wrong

The system FAILS LOUDLY with clear error messages. Common issues:

- Missing dependencies (install with `pip install -r requirements.txt`)
- Database connection issues (check your `.env` file)
- Data quality problems (check your YouTube data)

## 🛠️ Advanced usage

If you need more control, you can use the underlying system:

```python
from blueprint_execution_system import BlueprintExecutionManager

manager = BlueprintExecutionManager(Path("notebooks"))
result = manager.execute_complete_workflow()
```

But for most cases, just use the 🚀 play button!
