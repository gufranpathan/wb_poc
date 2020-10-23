"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mnameless` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``nameless.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``nameless.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import argparse
from .tasks import ImageToText,PdfToImage,ParseImage,AssemblyPdfToImage,PartParseImage,PdfDownload
import luigi
import logging
import multiprocessing


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

luigi.configuration.get_config().set('core', 'log_level', 'INFO')



parser = argparse.ArgumentParser(description='Command description.')
parser.add_argument("-a", "--assembly",required=False)
parser.add_argument("-t", "--part")
parser.add_argument("-g", "--page")
parser.add_argument("-d", "--dpi")
parser.add_argument("-l", "--lang")
parser.add_argument("-s", "--start_part")
parser.add_argument("-e", "--end_part")

parser.add_argument("-m","--method",default="simple",choices=["simple","cores","threads"])


def main(args=None):
    args = parser.parse_args(args=args)

    # luigi.build([PdfToImageOnS3(assembly=args.assembly, part=part) for part in range(1,10)],
    #             workers=multiprocessing.cpu_count(),
    #             local_scheduler=True)

    # luigi.build([AssemblyPdfToImage(assembly=int(args.assembly),
    #                                 start_part=int(args.start_part),
    #                                 end_part=int(args.end_part))],
    #             workers=6,
    #             local_scheduler=True)

    for part in range(int(args.start_part),int(args.end_part)+1):
        luigi.build([PartParseImage(assembly=int(args.assembly),
                                        start_part=int(part),
                                        end_part=int(part))],

                    workers=3,
                    local_scheduler=True)
    # for part in range(int(args.start_part),int(args.end_part)+1):
    #     luigi.build([PdfDownload(assembly=int(args.assembly),
    #                                     part=int(part))],
    #
    #                 # workers=2,
    #                 local_scheduler=True)




    # luigi.build([ParseImage(assembly=int(args.assembly),
    #                          part=int(args.part),
    #                         page=int(args.page),
    #                         lang=args.lang)],
    #             #workers=multiprocessing.cpu_count(),
    #             local_scheduler=True)

    # luigi.build([PdfToImage(assembly=int(args.assembly),
    #                          part=int(args.part))],
    #             #workers=multiprocessing.cpu_count(),
    #             local_scheduler=True)



    # if args.text:
    #     luigi.build([ImageToText(assembly=args.assembly,part=args.part)], local_scheduler=True)
    # else:
    #     luigi.build([ParseDocument(assembly=args.assembly, part=args.part,method=args.method)], local_scheduler=True)
