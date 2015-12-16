source("helper.R")

# Define server logic required to draw a histogram
shinyServer(function(input, output) {
    
  dataInput <- reactive({
    getWordCountDF(input$text_input, input$text_output, input$slider1)
  })  
  
  output$barPlot = renderPlot({
      dat = dataInput()
      if (!(is.data.frame(dat) && nrow(dat)==0)){
      dat= dat
      a=dat[1:10,]
      barplot(a$X2, ylim=c(0, max(a$X2)+100), 
              space = 0.5, ylab = "freq")
      text((1:10)*1.47, par("usr")[1]-200 , srt = 45, adj = 1,
           labels = a$X1, xpd = TRUE)
    }
  })
})
