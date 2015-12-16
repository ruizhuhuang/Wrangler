source("helper.R")

# Define server logic required to draw a histogram
shinyServer(function(input, output) {
    
  dataInput <- reactive({
    getWordCountDF(input$text_input, input$slider1)
  })  
  
  output$barPlot = renderPlot({
      dat = dataInput()
      if (!(is.data.frame(dat) && nrow(dat)==0)){
      dat= dat
      lower_bnd = input$slider2[1]
      upper_bnd = input$slider2[2]
      if (upper_bnd > nrow(dat)) upper_bnd = nrow(dat)
      interval = upper_bnd - lower_bnd +1
      a=dat[lower_bnd:upper_bnd,]
      barplot(a$X2, ylim=c(0, max(a$X2)+100), 
              space = 0.5, ylab = "freq")
      text(1:interval+((1:interval)-1)*0.5, par("usr")[1]-800/interval , srt = 45, adj = 1,
           labels = a$X1, xpd = TRUE)
    }
  })
})
