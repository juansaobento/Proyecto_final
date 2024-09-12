import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def envio_email(**context):
    subject = context["var"]["value"].get("subject_mail")
    from_address = context["var"]["value"].get("email")
    password = context["var"]["value"].get("email_password")
    to_address = context["var"]["value"].get("to_address")

    # Create a MIMEText object
    msg = MIMEMultipart()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    
    mensaje= 'El proceso de ETL, se ha realizado de manera exitosa'
    
    msg.attach(MIMEText(mensaje, 'plain'))
    
    try:
        
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Use your SMTP server and port
        server.starttls()  # Enable security
        
        server.login(from_address, password)

        text = msg.as_string()
        server.sendmail(from_address, to_address, text)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")