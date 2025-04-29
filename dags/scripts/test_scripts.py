from datetime import datetime

current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def action():
    print("Orhestartion completed, Date: "+current_date)
    
if __name__ == "__main__":
    action()