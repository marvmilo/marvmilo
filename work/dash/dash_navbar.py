from dash import html
import marvmiloTools as mmt

documentation_link = "https://wiki.bsh-sdd.com/display/MESPLAT/IFP+Forecasting"

#header spacer
html.Div(style = {"height": "120px"}),

#Header
html.Div(
    children = [
        #bosch look
        html.Div(
            style = {
                "background-image": "url(/assets/bosch.png)",
                "background-size": "cover",
                "height": "1rem"
            }
        ),
        
        #Navbar
        mmt.dash.nav.bar(
            logo = "url(/assets/BSH.png)",
            logo_style = {
                "width": "10rem", 
                "height": "3rem",
                "background-size": "cover",
            },
            title = "Joint Logfile Analytics",
            title_style = {"width": "8.5rem", "font-size": "1.5rem"},
            expand = "lg",
            items = [
                mmt.dash.nav.item.href(
                    "Documentation",
                    href = documentation_link,
                    target = "_blank",
                    size = "lg"
                ),
                mmt.dash.nav.item.normal(
                    "Problems?",
                    id = "problems-button",
                    size  = "lg"
                )
            ]
        )
    ],
    style = {
        "position": "fixed",
        "top": "0px",
        "width": "100%"
    }
)

#footer spacer
html.Div(style = {"height": "50px"}),

#bosch look footer
html.Div(
    style = {
        "background-image": "url(/assets/bosch.png)",
        "background-size": "cover",
        "height": "1rem",
        "position": "fixed",
        "bottom": "0px",
        "width": "100%",
        "box-shadow": "0 -0.5em 0.5em rgba(0, 0, 0, 0.5)"
    }
)