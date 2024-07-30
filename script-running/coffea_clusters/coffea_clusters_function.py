import awkward as ak
from hist import axis
import hist.dask as hda
from coffea.dataset_tools import preprocess

#Define the function that contains our physics logic
def ExampleAnalyzer(events):
    '''
    An example function that counts the events in files, adds one for each chunk processed, and fills a histogram with arbitrarily chosen data.

    Input:
        events: (Array) An array of data
    '''
    myAxis = axis.Regular(10,-0.5,9.5,name='data',label='example data')

    output = {}
    output = {
        "EventCount": 0,
        "Test": 0,
        "MyHist": hda.Hist(myAxis) #Create a dask.hist.Hist instead of regular Hist, since our function will run in delayed mode
    }

    #Do some physics here (make cuts, select objects, calculate masses, etc.)
    
    #Fill in output
    output["Test"] += 1
    output["EventCount"] += len(events)
    output["MyHist"].fill(ak.num(events.Jet)) #Histogram the number of jets in each event

    return output