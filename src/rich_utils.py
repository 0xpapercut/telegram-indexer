from rich.progress import Task, ProgressColumn
from rich.text import Text

class MessagesPerSecondColumn(ProgressColumn):
    def render(self, task: Task):
        speed = task.speed or 0
        return Text(f"{speed:.1f} msgs/s", style="bold bright_green")
