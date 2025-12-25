from pathlib import Path
import pandas as pd
import plotly.express as px


def SaveFig(fig, plot_path: Path, scale: int = 2) -> None:
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(plot_path), scale=scale)


def create_bar(df: pd.DataFrame, x_col: str, y_col: str, plot_title: str):
    sorted_data = df.sort_values(by=y_col, ascending=False)

    chart = px.bar(sorted_data,x=x_col,y=y_col,title=plot_title,)
    chart.update_layout(title_x=0.05,margin=dict(l=50, r=30, t=60, b=50),)
    chart.update_xaxes(title=x_col)
    chart.update_yaxes(title=y_col)
    return chart


def create_line(df: pd.DataFrame, x_col: str, y_col: str, plot_title: str):
    chart = px.line(df,x=x_col,y=y_col,title=plot_title,)
    chart.update_layout(title_x=0.05)
    chart.update_xaxes(title=x_col)
    chart.update_yaxes(title=y_col)

    return chart


def create_histogram(df: pd.DataFrame,column: str,bins: int =30 ,plot_title: str = ""):
    chart = px.histogram(df,x=column,nbins=bins,title=plot_title,)
    chart.update_layout(title_x=0.05)
    chart.update_xaxes(title=column)
    chart.update_yaxes(title="Count")

    return chart