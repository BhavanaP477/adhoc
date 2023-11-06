# Load required libraries
library(dplyr)
library(ggplot2)
library(MASS)
library(lattice)
library(gridExtra)
library(randomForest)
library(varImp)
library(stringr)
library(openxlsx)

# Read in and clean data
dat_raw <- read.csv("C:\\Users\\croff\\OneDrive - Kmart Australia Limited\\KHome\\Downloads\\neg_soh_new (3).csv")
names(dat_raw)[1] <- "LOCATION_CODE"
dat_raw$LOCATION_CODE <- factor(dat_raw$LOCATION_CODE, ordered = FALSE)
dat_raw$IS_DOSA_STORE <- factor(dat_raw$IS_DOSA_STORE, ordered = FALSE)
dat_clean <- dat_raw[
  (dat_raw$AVG_TOT_KEYCODES > 0) &
  (dat_raw$REGION_DESCRIPTION != "VANUATA"), ]

# Remove outliers: Retains about 96% of data
Q1 <- quantile(dat_clean$AVG_TOT_KEYCODES, probs = c(0.25, 0.75), na.rm = FALSE)
Q2 <- quantile(dat_clean$TOT_MANIFESTED_UNITS, probs = c(0.25, 0.75), na.rm = FALSE)
Q3 <- quantile(dat_clean$AVG_UNITS_PER_MANIFEST, probs = c(0.25, 0.75), na.rm = FALSE)
Q4 <- quantile(dat_clean$TOT_MANIFESTS, probs = c(0.25, 0.75), na.rm = FALSE)
iqr1 <- IQR(dat_clean$AVG_TOT_KEYCODES)
upr1 <- Q1[2] + 4.5 * iqr1
lwr1 <- Q1[1] - 4.5 * iqr1
iqr2 <- IQR(dat_clean$TOT_MANIFESTED_UNITS)
upr2 <- Q2[2] + 4.5 * iqr2
lwr2 <- Q2[1] - 4.5 * iqr2
iqr3 <- IQR(dat_clean$AVG_UNITS_PER_MANIFEST)
upr3 <- Q3[2] + 4.5 * iqr3
lwr3 <- Q3[1] - 4.5 * iqr3
iqr4 <- IQR(dat_clean$TOT_MANIFESTS)
upr4 <- Q4[2] + 4.5 * iqr4
lwr4 <- Q4[1] - 4.5 * iqr4
dat_final <- subset(
  dat_clean, 
  (dat_clean$AVG_TOT_KEYCODES < upr1) &
  (dat_clean$TOT_MANIFESTED_UNITS < upr2) &
  (dat_clean$AVG_UNITS_PER_MANIFEST < upr3) &
  (dat_clean$TOT_MANIFESTS < upr4)
)
nrow(dat_final)/nrow(dat_clean)

# EDA
p1 <- ggplot(dat_final, aes(x = REGION_DESCRIPTION, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  xlab("Region") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  coord_cartesian(ylim = c(0, 0.2))
p2 <- ggplot(dat_final, aes(x = RBU_DESCRIPTION, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  xlab("RBU") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  coord_cartesian(ylim = c(0, 0.2))
p3 <- ggplot(dat_final, aes(x = IS_DOSA_STORE, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  xlab("Is DOSA Store?") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  coord_cartesian(ylim = c(0, 0.2))
p4 <- ggplot(dat_final, aes(x = AVG_UNITS_PER_MANIFEST, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_point(color = "lightblue") +
  geom_smooth(color = "red", se = TRUE)  +
  scale_y_continuous(limits = c(0, 0.2)) +
  xlab("Avg. Units per Manifest") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)")
p5 <- ggplot(dat_final, aes(x = TOT_MANIFESTS, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_point(color = "lightblue") +
  geom_smooth(color = "red", se = TRUE)  +
  scale_y_continuous(limits = c(0, 0.2)) +
  xlab("Total Number of Manifests") +  
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
p6 <- ggplot(dat_final, aes(x = TYPE, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  xlab("Apparel vs. GM") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  coord_cartesian(ylim = c(0, 0.2))
grid.arrange(p1, p3, nrow = 1)
grid.arrange(p6, p2, nrow = 1)
grid.arrange(p5, p4, nrow = 1)

# Redefine variables with respect to EDA interpretations
dat_final$TOT_MANIFESTS_ORDINAL <- rep(NA, nrow(dat_final))
dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL <- rep(NA, nrow(dat_final))
for (i in 1:nrow(dat_final)){
  if (dat_final$TOT_MANIFESTS[i] < 500){
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "0-500"
  } else if (500 <= dat_final$TOT_MANIFESTS[i] & dat_final$TOT_MANIFESTS[i] < 1000){
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "500-1000"
  } else if (1000 <= dat_final$TOT_MANIFESTS[i] & dat_final$TOT_MANIFESTS[i] < 1500) {
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "1000-1500"
  } else if (1500 <= dat_final$TOT_MANIFESTS[i] & dat_final$TOT_MANIFESTS[i] < 2000) {
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "1500-2000"
  } else if (2000 <= dat_final$TOT_MANIFESTS[i] & dat_final$TOT_MANIFESTS[i] < 2500) {
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "2000-2500"
  } else if (2500 <= dat_final$TOT_MANIFESTS[i] & dat_final$TOT_MANIFESTS[i] < 3000) {
    dat_final$TOT_MANIFESTS_ORDINAL[i] = "2500-3000"  
  } else if (3000 <= dat_final$TOT_MANIFESTS[i]) {
    dat_final$TOT_MANIFESTS_ORDINAL[i] = ">=3000" 
  }
}
for (i in 1:nrow(dat_final)){
  if (dat_final$AVG_UNITS_PER_MANIFEST[i] < 5){
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = "0-5"
  } else if (5 <= dat_final$AVG_UNITS_PER_MANIFEST[i] & dat_final$AVG_UNITS_PER_MANIFEST[i] < 10){
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = "5-10"
  } else if (10 <= dat_final$AVG_UNITS_PER_MANIFEST[i] & dat_final$AVG_UNITS_PER_MANIFEST[i] < 15) {
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = "10-15"
  } else if (15 <= dat_final$AVG_UNITS_PER_MANIFEST[i] & dat_final$AVG_UNITS_PER_MANIFEST[i] < 20) {
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = "15-20"
  } else if (20 <= dat_final$AVG_UNITS_PER_MANIFEST[i] & dat_final$AVG_UNITS_PER_MANIFEST[i] < 25) {
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = "20-25"
  } else if (25 <= dat_final$AVG_UNITS_PER_MANIFEST[i]) {
    dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL[i] = ">=25"  
  }
}
dat_final$TOT_MANIFESTS_ORDINAL <- factor(
  dat_final$TOT_MANIFESTS_ORDINAL, 
  ordered = TRUE, 
  levels = c("0-500", "500-1000", "1000-1500", "1500-2000", "2000-2500", "2500-3000", ">=3000"))
dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL <- factor(
  dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL, 
  ordered = TRUE, 
  levels = c("0-5", "5-10", "10-15", "15-20", "20-25", ">=25"))
dat_final$TOT_MANIFESTS_CAT <- factor(dat_final$TOT_MANIFESTS_ORDINAL, ordered = FALSE)
dat_final$AVG_UNITS_PER_MANIFEST_CAT <- factor(dat_final$AVG_UNITS_PER_MANIFEST_ORDINAL, ordered = FALSE)
p7 <- ggplot(dat_final, aes(x = TOT_MANIFESTS_ORDINAL, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  stat_summary(fun = median, geom = "line", aes(group=1), color = "red", lwd = 1) + 
  stat_summary(fun = median, geom = "point") +
  xlab("Total Manifests (Bucketted)") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  coord_cartesian(ylim = c(0, 0.2)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
p8 <- ggplot(dat_final, aes(x = AVG_UNITS_PER_MANIFEST_ORDINAL, y = AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES)) +
  geom_boxplot() +
  stat_summary(fun = median, geom = "line", aes(group=1), color = "red", lwd = 1) + 
  stat_summary(fun = median, geom = "point") +
  xlab("Avg. Units per Manifest (Bucketted)") +
  ylab("Average Prop. of Negative SOH Keycodes (Over Time)") +
  coord_cartesian(ylim = c(0, 0.2)) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
grid.arrange(p7, p8, nrow = 1)

# Split data into training and test sets
set.seed(1234)
train_sample <- floor(0.80 * nrow(dat_final))
train_ind <- sample(seq_len(nrow(dat_final)), size = train_sample)
dat_train <- dat_final[train_ind, ]
dat_test <- dat_final[-train_ind, ]

# Weighted OLS regression
mod1 <- glm(
  AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES ~ 
    REGION_DESCRIPTION + 
    TYPE +
    RBU_DESCRIPTION + 
    IS_DOSA_STORE +
    TOT_MANIFESTS +
    AVG_UNITS_PER_MANIFEST,
  weights = AVG_TOT_KEYCODES,
  data = dat_train
)
anova(mod1, test = "Chisq")
summary(mod1)
logistic_reg_results <- as.data.frame(summary(mod1)$coefficients)
write.xlsx(logistic_reg_results, "C:\\Users\\croff\\Desktop\\neg_soh_logistic_reg.xlsx", rowNames = TRUE)

# Random Forest Regressor
mod2 <- randomForest(
  AVG_NEGATIVE_SOH_KEYCODES / AVG_TOT_KEYCODES ~ 
    REGION_DESCRIPTION + 
    TYPE +
    RBU_DESCRIPTION + 
    IS_DOSA_STORE +
    TOT_MANIFESTS +
    AVG_UNITS_PER_MANIFEST,
  importance = TRUE, 
  data = dat_train,
  ntree = 100
)
importance(mod2, type = 1)
varImpPlot(mod2, type = 1, main = "Feature Importance (By Random Forest)")

# Assess prediction accuracy of the fitted models above
y_true <- dat_test$AVG_NEGATIVE_SOH_KEYCODES / dat_test$AVG_TOT_KEYCODES
y_pred_mod1 <- predict(mod1, newdata = dat_test)
y_pred_mod2 <- predict(mod2, newdata = dat_test)
MAE(y_true, y_pred_mod1)
MAE(y_true, y_pred_mod2)