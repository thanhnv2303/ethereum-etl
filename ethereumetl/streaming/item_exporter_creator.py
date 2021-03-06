#  MIT License
#
#  Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.knowledge_graph_exporter import KnowledgeGraphExporter


def create_item_exporter(output):
    item_exporter_type = determine_item_exporter_type(output)
    if item_exporter_type == ItemExporterType.CONSOLE:
        item_exporter = ConsoleItemExporter()
    elif item_exporter_type == ItemExporterType.KNOWLEDGE_GRAPH:
        item_exporter = KnowledgeGraphExporter()
    else:
        raise ValueError('Unable to determine item exporter type for output ' + output)

    return item_exporter


def determine_item_exporter_type(output):
    if output is not None and output.startswith('knowledge_graph'):
        return ItemExporterType.KNOWLEDGE_GRAPH
    elif output is None or output == 'console':
        return ItemExporterType.CONSOLE
    else:
        return ItemExporterType.UNKNOWN


class ItemExporterType:
    PUBSUB = 'pubsub'
    POSTGRES = 'postgres'
    CONSOLE = 'console'
    KNOWLEDGE_GRAPH = 'knowledge_graph'
    UNKNOWN = 'unknown'
