from multiprocessing import Pool, TimeoutError
import arcpy
from arcpy import env
from arcpy.sa import *
import os
#
arcpy.CheckOutExtension("Spatial")
def TesseLSpatial(dirR):
    address=r'C:\Users\Para2x\Desktop\RegionTest'
    arcpy.env.overwriteOutput = True
    arcpy.env.parallelProcessingFactor="100%"
    arcpy.env.workspace=os.path.join(address)
    os.chdir(arcpy.env.workspace)
    ################################
    fc = arcpy.ListFeatureClasses("*_Dissolved*")[0]
    arcpy.MakeFeatureLayer_management(fc,"dissolved_Layer")
    name=fc[0:2].upper()
    OIDField = arcpy.Describe(fc).OIDFieldName # get OID/FID field name
    auxfold="test"
    ###
    #if not os.path.exists("test")
    #    os.mkdir(auxfold)
    pocursor = arcpy.SearchCursor ("dissolved_Layer")
    try:
        for porow in pocursor:
            print name+"->"+"FID"+str (porow.getValue(OIDField))
            ##################### Subsetting
            sql = '"' + OIDField + '" = ' + str (porow.getValue(OIDField)) #SQL to select one feature
            arcpy.SelectLayerByAttribute_management ("dissolved_Layer", "", sql) #Select polygon feature by OID
            memoryFeature=auxfold+"\\"+name + "_FID_" + str (porow.getValue(OIDField))+".shp"
            arcpy.CopyFeatures_management("dissolved_Layer", memoryFeature)
            arcpy.MakeFeatureLayer_management(memoryFeature,"selected_Poly")
            ############## Tessleation
            description = arcpy.Describe(memoryFeature)
            extent = description.extent
            memoryFeature_Tes=auxfold+"\\"+name + "_Tessel_" + str (porow.getValue(OIDField))+".shp"
            arcpy.GenerateTessellation_management(memoryFeature_Tes, extent, "HEXAGON", "11 SquareMiles")
            ############### Clip
            memoryFeature_Tes_clip = auxfold + "\\" + "Tessel_cliped_"+ str (porow.getValue(OIDField))+".shp"
            arcpy.Clip_analysis(memoryFeature_Tes, memoryFeature, memoryFeature_Tes_clip)
            ############### Spatial Join
            Tes_spatialJoin=auxfold+"\\"+name + "_Tessel_Joined_" + str (porow.getValue(OIDField))+".shp"
            arcpy.SpatialJoin_analysis(memoryFeature_Tes_clip, "selected_Poly", Tes_spatialJoin)
            del porow
            arcpy.Delete_management(memoryFeature)
            arcpy.Delete_management(memoryFeature_Tes)
            arcpy.Delete_management(memoryFeature_Tes_clip)
        del pocursor
        arcpy.env.workspace=os.path.join(address,auxfold)
        print arcpy.env.workspace
        arcpy.Merge_management(arcpy.ListFeatureClasses("*"),name+"_merged.shp")
        # Calculate centroid
        arcpy.AddField_management(name+"_merged.shp", "xCentroid", "DOUBLE", 18, 11)
        arcpy.AddField_management(name+"_merged.shp", "yCentroid", "DOUBLE", 18, 11)
        arcpy.CalculateField_management(name+"_merged.shp", "xCentroid", "!SHAPE.CENTROID.X!", "PYTHON_9.3")
        arcpy.CalculateField_management(name+"_merged.shp", "yCentroid", "!SHAPE.CENTROID.Y!", "PYTHON_9.3")
    except Exception as e:
        print "Error processing", fc
        print "Error", e
#############################################
if __name__ == '__main__':
    address=r'C:\Users\Para2x\Desktop\RegionTest'
    pool = Pool(1)               # start 4 worker processes
    #####################
    os.chdir(address)
    directories = [ x for x in os.listdir('.') if os.path.isdir(x) ] # finding all the folders
    #del directories[0]## removing the just soil folder
    ###################### [directories[index] for index in [0] ]
    pool.map(TesseLSpatial, directories)
    pool.close()
    pool.join()
