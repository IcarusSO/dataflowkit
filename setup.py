from distutils.core import setup
setup(
  name = 'dataflowkit',
  packages = ['dataflowkit', 'dataflowkit.graphs', 'dataflowkit.datasets', 'dataflowkit.recipes', 'dataflowkit.utils'], # this must be the same as the name above
  version = '0.1.5',
  description = 'A dataflow management framework for datascientists and engineers',
  author = 'Icarus So',
  author_email = 'icarus.so@gmail.com',
  url = 'https://github.com/IcarusSO/dataflowkit', # use the URL to the github repo
  download_url = 'https://github.com/IcarusSO/dataflowkit/tarball/0.1.5', # I'll explain this in a second
  keywords = ['dataflow', 'data', 'flow', 'management'], # arbitrary keywords
  classifiers = [],
)
