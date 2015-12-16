
# Define UI for application that draws a histogram
shinyUI(fluidPage(
  
  # Application title
  titlePanel("Word Count - Spark"),
  
  fluidRow(
    column(3, 
           textInput("text_input", label = h3("Input"), 
                     value = "/user/rhuang/data/book.txt")),
    column(3, 
           textInput("text_output", label = h3("Output"), 
                     value = "/user/rhuang/output/test1")),
    column(3, 
           sliderInput("slider1", label = h3("Number of executor"),
                       min = 1, max = 200, value = 10)
    ),
    column(3, 
           h3("Buttons"),submitButton("Submit")
           
    )       
  ),
  
  
  fluidRow(
    mainPanel(
      plotOutput("barPlot")
    )
  )
 
))