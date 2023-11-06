# Load required libraries
library(ggplot2)
library(gridExtra)
library(dplyr)

# Read in data
df_raw <- read.csv("C:\\Users\\croff\\OneDrive - Kmart Australia Limited\\KHome\\Downloads\\neg_soh_outstanding_manifest.csv")
df_raw$HAS_MANIFESTS_10_DAYS <- ifelse(df_raw$OUTSTANDING_MANIFEST_UNITS_10_DAYS > 0, "Yes", "No")
df_raw$HAS_MANIFESTS_30_DAYS <- ifelse(df_raw$OUTSTANDING_MANIFEST_UNITS_30_DAYS > 0, "Yes", "No")

# Remove Outliers
Q1 <- quantile(df_raw$OUTSTANDING_MANIFEST_UNITS_CURRENT, probs = c(0.25, 0.75), na.rm = FALSE)
iqr1 <- IQR(df_raw$OUTSTANDING_MANIFEST_UNITS_CURRENT)
upr1 <- Q1[2] + 1.5 * iqr1
lwr1 <- Q1[1] - 1.5 * iqr1
df_final <- subset(
  df_raw,
    (df_raw$OUTSTANDING_MANIFEST_UNITS_CURRENT < upr1) 
)

# EDA
p1 <- ggplot(data = df_final, aes(x = OUTSTANDING_MANIFEST_UNITS_CURRENT, y = abs(NEG_SOH_QTY))) +
  geom_point() +
  geom_smooth(method = "lm", se = FALSE) +
  xlab("Outstanding Manifest Units (Current)") +
  ylab("Neg. SOH Units")


# Lin. regression
mod1 <- lm(abs(NEG_SOH_QTY) ~ OUTSTANDING_MANIFEST_UNITS_CURRENT, data = df_final)
est <- round(summary(mod1)$coef[2], 2)
lwr <- round(confint(mod1)[2,1], 2)
upr <- round(confint(mod1)[2,2], 2)

# Test model assumptions
par(mfrow = c(1,2))
plot(mod1, which = 1:2)

# Estimate of outstanding manifest contribution to neg. SOH
tot_neg_soh_cost <- sum(df_raw$NEG_SOH_COST)
lwr_dollar_contribution <- lwr * tot_neg_soh_cost
est_dollar_contribution <- est * tot_neg_soh_cost
upr_dollar_contribution <- upr * tot_neg_soh_cost
print(cbind(lwr_dollar_contribution, est_dollar_contribution, upr_dollar_contribution))

