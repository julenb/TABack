from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.graphics.barcode import code39






def creaPDF(nombre_pdf,nombreEvento,fecha,lugar,hora,duracion,precio,numPaginas):
    # Crear un PDF
    doc = SimpleDocTemplate(nombre_pdf, pagesize=letter)
    
    contenido = []
    for i in range(numPaginas):
        print('pagina '+str(i))
        
        # Establecer estilos
        estilos = getSampleStyleSheet()
        estilo_titulo = estilos["Title"]
        estilo_normal = estilos["Normal"]

        # Título del concierto
        titulo = Paragraph(nombreEvento, estilo_titulo)
        contenido.append(titulo)

        # Agregar una línea horizontal
        contenido.append(Spacer(1, 24))
        contenido.append(Image('imagen_concierto.jpg', width=400, height=200))
        contenido.append(Spacer(1, 24))

        # Detalles del concierto
        detalles = [
            ("Nombre del evento:", nombreEvento),
            ("Fecha:", fecha),
            ("Lugar:", lugar),
            ("Hora de inicio:", hora),
            ("Duración:", duracion),
            ("Precio:", precio),
        ]

        tabla_detalles = Table(detalles, colWidths=[150, 200])
        tabla_detalles.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('SIZE', (0, 0), (-1, -1), 12),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('TOPPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))

        contenido.append(tabla_detalles)
        contenido.append(Spacer(1, 24))

        # Información adicional
        informacion_adicional = Paragraph(
            "¡Únete a nosotros para una noche mágica llena de música clásica interpretada por Quevedo! "
            "Este concierto promete una experiencia inolvidable para los amantes de la música.",
            estilo_normal
        )
        contenido.append(informacion_adicional)
        contenido.append(Spacer(1, 24))
        # Romper la página para la siguiente sección

        # Código de barras
        barcode = code39.Extended39(nombre_pdf, barWidth=0.6, barHeight=40)
        barcode.hAlign = 'CENTER'
        contenido.append(barcode)
        contenido.append(PageBreak()) 
        # Guardar el PDF
    doc.build(contenido)

    print(f"Se ha creado el archivo PDF: {nombre_pdf}")


